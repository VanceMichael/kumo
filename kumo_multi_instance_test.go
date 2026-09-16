package kumo_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/sivchari/kumo"
)

const testRegion = "us-east-1"

func testAWSConfig(t *testing.T) aws.Config {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(context.Background(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	return cfg
}

func sqsClient(cfg aws.Config, url string) *sqs.Client {
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(url)
	})
}

func snsClient(cfg aws.Config, url string) *sns.Client {
	return sns.NewFromConfig(cfg, func(o *sns.Options) {
		o.BaseEndpoint = aws.String(url)
	})
}

func s3Client(cfg aws.Config, url string) *s3.Client {
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(url)
		o.UsePathStyle = true
	})
}

func cloudwatchClient(cfg aws.Config, url string) *cloudwatch.Client {
	return cloudwatch.NewFromConfig(cfg, func(o *cloudwatch.Options) {
		o.BaseEndpoint = aws.String(url)
	})
}

func lambdaClient(cfg aws.Config, url string) *lambda.Client {
	return lambda.NewFromConfig(cfg, func(o *lambda.Options) {
		o.BaseEndpoint = aws.String(url)
	})
}

// mustCreateQueue creates an SQS queue and returns its URL and ARN.
func mustCreateQueue(t *testing.T, ctx context.Context, c *sqs.Client, name string) (url, arn string) {
	t.Helper()

	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name)})
	if err != nil {
		t.Fatalf("CreateQueue(%s): %v", name, err)
	}

	url = aws.ToString(out.QueueUrl)

	attrs, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes(%s): %v", name, err)
	}

	return url, attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
}

// receiveOnce runs one short-poll ReceiveMessage.
func receiveOnce(t *testing.T, ctx context.Context, c *sqs.Client, queueURL string) []sqstypes.Message {
	t.Helper()

	out, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     1,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage(%s): %v", queueURL, err)
	}

	return out.Messages
}

// waitForMessage polls until a message whose body contains substr
// arrives or the deadline elapses. Received messages are not deleted so
// failed assertions still surface the queue contents.
func waitForMessage(t *testing.T, ctx context.Context, c *sqs.Client, queueURL, substr string) sqstypes.Message {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		for _, m := range receiveOnce(t, ctx, c, queueURL) {
			if substr == "" || strings.Contains(aws.ToString(m.Body), substr) {
				return m
			}
		}
	}

	t.Fatalf("no message containing %q arrived on %s", substr, queueURL)

	return sqstypes.Message{}
}

// assertQueueEmpty proves no delivery reached the queue, waiting long
// enough to catch any cross-instance event that is merely delayed.
func assertQueueEmpty(t *testing.T, ctx context.Context, c *sqs.Client, queueURL string) {
	t.Helper()

	// Two long-polls (2s total) cover the window in which the other
	// instance's delivery goroutine could still be in flight.
	for range 2 {
		if msgs := receiveOnce(t, ctx, c, queueURL); len(msgs) != 0 {
			bodies := make([]string, 0, len(msgs))
			for _, m := range msgs {
				bodies = append(bodies, aws.ToString(m.Body))
			}

			t.Fatalf("queue %s unexpectedly received %d message(s): %s", queueURL, len(msgs), strings.Join(bodies, "\n"))
		}
	}
}

// uniqueSuffix keeps resources from colliding across repeated test runs
// against a persistent emulator; the isolation checks themselves use the
// SAME names on both instances.
func uniqueSuffix() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

// TestMultiServer_IndependentStorageAndEndpoints is the core isolation
// contract: two NewServer calls side by side must not share data, and
// every resource URL they return must point at that server's own
// random listener rather than localhost:4566.
func TestMultiServer_IndependentStorageAndEndpoints(t *testing.T) {
	ctx := context.Background()

	srvA := kumo.NewServer()
	srvB := kumo.NewServer()

	defer srvA.Close()
	defer srvB.Close()

	if srvA.URL == srvB.URL {
		t.Fatalf("two servers must listen on different URLs, both are %s", srvA.URL)
	}

	cfg := testAWSConfig(t)
	sqsA, sqsB := sqsClient(cfg, srvA.URL), sqsClient(cfg, srvB.URL)
	s3A, s3B := s3Client(cfg, srvA.URL), s3Client(cfg, srvB.URL)
	snsA, snsB := snsClient(cfg, srvA.URL), snsClient(cfg, srvB.URL)

	suffix := uniqueSuffix()
	queueName := "iso-queue-" + suffix
	bucketName := "iso-bucket-" + suffix
	topicName := "iso-topic-" + suffix

	// --- SQS: same queue name on both instances ---
	urlA, _ := mustCreateQueue(t, ctx, sqsA, queueName)
	urlB, _ := mustCreateQueue(t, ctx, sqsB, queueName)

	if !strings.HasPrefix(urlA, srvA.URL+"/") {
		t.Errorf("instance A QueueUrl = %q, want prefix %q", urlA, srvA.URL)
	}

	if !strings.HasPrefix(urlB, srvB.URL+"/") {
		t.Errorf("instance B QueueUrl = %q, want prefix %q", urlB, srvB.URL)
	}

	if strings.Contains(urlA, "localhost:4566") || strings.Contains(urlB, "localhost:4566") {
		t.Errorf("QueueUrl must not fall back to the default port: %q / %q", urlA, urlB)
	}

	_, err := sqsA.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(urlA),
		MessageBody: aws.String("only-instance-A"),
	})
	if err != nil {
		t.Fatalf("SendMessage A: %v", err)
	}

	msg := waitForMessage(t, ctx, sqsA, urlA, "only-instance-A")
	_, _ = sqsA.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(urlA), ReceiptHandle: msg.ReceiptHandle})

	assertQueueEmpty(t, ctx, sqsB, urlB)

	// --- S3: same bucket name on both instances ---
	if _, err := s3A.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)}); err != nil {
		t.Fatalf("CreateBucket A: %v", err)
	}

	if _, err := s3B.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)}); err != nil {
		t.Fatalf("CreateBucket B: %v", err)
	}

	key := "shared-key"

	if _, err := s3A.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("a-payload")),
	}); err != nil {
		t.Fatalf("PutObject A: %v", err)
	}

	if _, err := s3A.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucketName), Key: aws.String(key)}); err != nil {
		t.Errorf("HeadObject on instance A should succeed: %v", err)
	}

	var notFound *s3types.NotFound
	if _, err := s3B.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucketName), Key: aws.String(key)}); !errors.As(err, &notFound) {
		t.Errorf("HeadObject on instance B should report NotFound, got %v", err)
	}

	// --- SNS: topic created only on A must not be visible on B ---
	if _, err := snsA.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(topicName)}); err != nil {
		t.Fatalf("CreateTopic A: %v", err)
	}

	topicsB, err := snsB.ListTopics(ctx, &sns.ListTopicsInput{})
	if err != nil {
		t.Fatalf("ListTopics B: %v", err)
	}

	for _, tp := range topicsB.Topics {
		if strings.HasSuffix(aws.ToString(tp.TopicArn), ":"+topicName) {
			t.Fatalf("topic created on instance A leaked into instance B: %s", aws.ToString(tp.TopicArn))
		}
	}
}

// TestMultiServer_LocalEventsStayInInstance verifies that events an
// instance produces are only delivered to targets owned by that same
// instance: SNS -> SQS, S3 -> SQS, S3 -> SNS -> SQS and
// CloudWatch alarm -> SNS -> SQS.
func TestMultiServer_LocalEventsStayInInstance(t *testing.T) {
	ctx := context.Background()

	srvA := kumo.NewServer()
	srvB := kumo.NewServer()

	defer srvA.Close()
	defer srvB.Close()

	cfg := testAWSConfig(t)

	sqsA, sqsB := sqsClient(cfg, srvA.URL), sqsClient(cfg, srvB.URL)
	snsA := snsClient(cfg, srvA.URL)
	s3A, s3B := s3Client(cfg, srvA.URL), s3Client(cfg, srvB.URL)
	cwA := cloudwatchClient(cfg, srvA.URL)

	suffix := uniqueSuffix()

	// Identically named topics and queues on both instances.
	topicAOut, err := snsA.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("events-topic-" + suffix)})
	if err != nil {
		t.Fatalf("CreateTopic A: %v", err)
	}

	topicA := aws.ToString(topicAOut.TopicArn)

	// B must have its own topic with the same local name.
	topicBOut, err := snsClient(cfg, srvB.URL).CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("events-topic-" + suffix)})
	if err != nil {
		t.Fatalf("CreateTopic B: %v", err)
	}

	topicB := aws.ToString(topicBOut.TopicArn)

	// Topic ARNs are account/region/name based and therefore identical
	// across instances; isolation comes from each instance holding its
	// own topic/queue storage, not from ARN namespacing.
	_ = topicB

	snsQueueA, snsQueueARN_A := mustCreateQueue(t, ctx, sqsA, "sns-queue-"+suffix)
	snsQueueB, snsQueueARN_B := mustCreateQueue(t, ctx, sqsB, "sns-queue-"+suffix)

	if _, err := snsA.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(topicA),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(snsQueueARN_A),
	}); err != nil {
		t.Fatalf("Subscribe A: %v", err)
	}

	if _, err := snsClient(cfg, srvB.URL).Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(topicB),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(snsQueueARN_B),
	}); err != nil {
		t.Fatalf("Subscribe B: %v", err)
	}

	// 1) SNS -> SQS delivery stays on A.
	if _, err := snsA.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicA),
		Message:  aws.String("sns-message-"+suffix),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	waitForMessage(t, ctx, sqsA, snsQueueA, "sns-message-"+suffix)
	assertQueueEmpty(t, ctx, sqsB, snsQueueB)

	// 2) S3 -> SQS and S3 -> SNS -> SQS stay on A. Separate buckets are
	// used because kumo rejects overlapping event rules within one bucket.
	sqsBucketName := "events-bucket-sqs-" + suffix
	snsBucketName := "events-bucket-sns-" + suffix

	for _, b := range []string{sqsBucketName, snsBucketName} {
		if _, err := s3A.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil {
			t.Fatalf("CreateBucket A %s: %v", b, err)
		}

		if _, err := s3B.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil {
			t.Fatalf("CreateBucket B %s: %v", b, err)
		}
	}

	s3QueueA, s3QueueARN_A := mustCreateQueue(t, ctx, sqsA, "s3-sqs-queue-"+suffix)
	s3QueueB, _ := mustCreateQueue(t, ctx, sqsB, "s3-sqs-queue-"+suffix)

	s3TopicQueueA, s3TopicQueueARN_A := mustCreateQueue(t, ctx, sqsA, "s3-sns-queue-"+suffix)
	s3TopicQueueB, _ := mustCreateQueue(t, ctx, sqsB, "s3-sns-queue-"+suffix)

	s3TopicOut, err := snsA.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("s3-events-topic-" + suffix)})
	if err != nil {
		t.Fatalf("CreateTopic s3 A: %v", err)
	}

	if _, err := snsA.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: s3TopicOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(s3TopicQueueARN_A),
	}); err != nil {
		t.Fatalf("Subscribe s3-topic A: %v", err)
	}

	if _, err := s3A.PutBucketNotificationConfiguration(ctx,
		&s3.PutBucketNotificationConfigurationInput{
			Bucket: aws.String(sqsBucketName),
			NotificationConfiguration: &s3types.NotificationConfiguration{
				QueueConfigurations: []s3types.QueueConfiguration{{
					Id:       aws.String("to-sqs"),
					QueueArn: aws.String(s3QueueARN_A),
					Events:   []s3types.Event{s3types.EventS3ObjectCreatedPut},
				}},
			},
		},
	); err != nil {
		t.Fatalf("PutBucketNotificationConfiguration(sqs): %v", err)
	}

	if _, err := s3A.PutBucketNotificationConfiguration(ctx,
		&s3.PutBucketNotificationConfigurationInput{
			Bucket: aws.String(snsBucketName),
			NotificationConfiguration: &s3types.NotificationConfiguration{
				TopicConfigurations: []s3types.TopicConfiguration{{
					Id:       aws.String("to-sns"),
					TopicArn: s3TopicOut.TopicArn,
					Events:   []s3types.Event{s3types.EventS3ObjectCreatedPut},
				}},
			},
		},
	); err != nil {
		t.Fatalf("PutBucketNotificationConfiguration(sns): %v", err)
	}

	if _, err := s3A.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(sqsBucketName),
		Key:    aws.String("object-on-A"),
		Body:   bytes.NewReader([]byte("hello")),
	}); err != nil {
		t.Fatalf("PutObject A (sqs bucket): %v", err)
	}

	// The direct S3 -> SQS event.
	msg := waitForMessage(t, ctx, sqsA, s3QueueA, sqsBucketName)
	_, _ = sqsA.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(s3QueueA), ReceiptHandle: msg.ReceiptHandle})

	if _, err := s3A.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(snsBucketName),
		Key:    aws.String("object-on-A"),
		Body:   bytes.NewReader([]byte("hello")),
	}); err != nil {
		t.Fatalf("PutObject A (sns bucket): %v", err)
	}

	// The S3 -> SNS -> SQS event (wrapped twice in envelopes).
	waitForMessage(t, ctx, sqsA, s3TopicQueueA, "ObjectCreated:Put")

	// Nothing of this should reach B, even though every resource name matches.
	assertQueueEmpty(t, ctx, sqsB, s3QueueB)
	assertQueueEmpty(t, ctx, sqsB, s3TopicQueueB)

	// 3) CloudWatch alarm action -> SNS -> SQS stays on A.
	alarmName := "iso-alarm-" + suffix
	cwQueueA, cwQueueARN_A := mustCreateQueue(t, ctx, sqsA, "cw-queue-"+suffix)
	cwQueueB, _ := mustCreateQueue(t, ctx, sqsB, "cw-queue-"+suffix)

	cwTopicOut, err := snsA.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("cw-topic-" + suffix)})
	if err != nil {
		t.Fatalf("CreateTopic cw A: %v", err)
	}

	if _, err := snsA.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: cwTopicOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(cwQueueARN_A),
	}); err != nil {
		t.Fatalf("Subscribe cw A: %v", err)
	}

	if _, err := cwA.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{
		AlarmName:          aws.String(alarmName),
		MetricName:         aws.String("m"),
		Namespace:          aws.String("kumo/test"),
		Statistic:          cwtypes.StatisticAverage,
		Period:             aws.Int32(60),
		EvaluationPeriods:  aws.Int32(1),
		Threshold:          aws.Float64(1),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		ActionsEnabled:     aws.Bool(true),
		AlarmActions:       []string{aws.ToString(cwTopicOut.TopicArn)},
	}); err != nil {
		t.Fatalf("PutMetricAlarm: %v", err)
	}

	if _, err := cwA.SetAlarmState(ctx, &cloudwatch.SetAlarmStateInput{
		AlarmName:   aws.String(alarmName),
		StateValue:  cwtypes.StateValueAlarm,
		StateReason: aws.String("multi-instance isolation test"),
	}); err != nil {
		t.Fatalf("SetAlarmState: %v", err)
	}

	waitForMessage(t, ctx, sqsA, cwQueueA, alarmName)
	assertQueueEmpty(t, ctx, sqsB, cwQueueB)
}

// TestMultiServer_LambdaLocationUsesInstanceAddress verifies the
// function code address handed back to clients points at the owning
// instance's listener.
func TestMultiServer_LambdaLocationUsesInstanceAddress(t *testing.T) {
	ctx := context.Background()

	srvA := kumo.NewServer()
	srvB := kumo.NewServer()

	defer srvA.Close()
	defer srvB.Close()

	cfg := testAWSConfig(t)
	lambdaA, lambdaB := lambdaClient(cfg, srvA.URL), lambdaClient(cfg, srvB.URL)

	fnName := "iso-fn-" + uniqueSuffix()
	code := &lambdatypes.FunctionCode{ZipFile: []byte("PK\x03\x04dummy")}

	if _, err := lambdaA.CreateFunction(ctx, &lambda.CreateFunctionInput{
		FunctionName: aws.String(fnName),
		Runtime:      lambdatypes.RuntimeProvidedal2,
		Role:         aws.String("arn:aws:iam::000000000000:role/exec"),
		Handler:      aws.String("bootstrap"),
		Code:         code,
	}); err != nil {
		t.Fatalf("CreateFunction A: %v", err)
	}

	if _, err := lambdaB.CreateFunction(ctx, &lambda.CreateFunctionInput{
		FunctionName: aws.String(fnName),
		Runtime:      lambdatypes.RuntimeProvidedal2,
		Role:         aws.String("arn:aws:iam::000000000000:role/exec"),
		Handler:      aws.String("bootstrap"),
		Code:         code,
	}); err != nil {
		t.Fatalf("CreateFunction B: %v", err)
	}

	outA, err := lambdaA.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(fnName)})
	if err != nil {
		t.Fatalf("GetFunction A: %v", err)
	}

	locA := aws.ToString(outA.Code.Location)
	if !strings.HasPrefix(locA, srvA.URL+"/lambda-code/") {
		t.Errorf("function A Location = %q, want prefix %q/lambda-code/", locA, srvA.URL)
	}

	if strings.Contains(locA, "localhost:4566") {
		t.Errorf("function Location must not fall back to default port: %q", locA)
	}

	outB, err := lambdaB.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(fnName)})
	if err != nil {
		t.Fatalf("GetFunction B: %v", err)
	}

	locB := aws.ToString(outB.Code.Location)
	if !strings.HasPrefix(locB, srvB.URL+"/lambda-code/") {
		t.Errorf("function B Location = %q, want prefix %q/lambda-code/", locB, srvB.URL)
	}
}

// TestMultiServer_CloseIsIndependentAndIdempotent verifies that closing
// one instance releases its own work, never affects a sibling instance,
// and is panic-free under repeated and concurrent Close calls.
func TestMultiServer_CloseIsIndependentAndIdempotent(t *testing.T) {
	ctx := context.Background()

	cfg := testAWSConfig(t)

	srvA := kumo.NewServer()
	srvB := kumo.NewServer()

	sqsA := sqsClient(cfg, srvA.URL)
	sqsB := sqsClient(cfg, srvB.URL)

	if _, err := sqsA.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("before-close-" + uniqueSuffix())}); err != nil {
		t.Fatalf("CreateQueue A: %v", err)
	}

	if _, err := sqsB.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("before-close-" + uniqueSuffix())}); err != nil {
		t.Fatalf("CreateQueue B: %v", err)
	}

	// Repeated Close on A: must be a no-op after the first.
	srvA.Close()
	srvA.Close()

	// Concurrent Close on A must not panic on shared channels/resources.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)

		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic during concurrent Close: %v", r)
				}
			}()

			srvA.Close()
		}()
	}

	wg.Wait()

	// A is no longer serving.
	if resp, err := http.Get(srvA.URL + "/health"); err == nil {
		_ = resp.Body.Close()
		t.Fatalf("instance A should be closed and refuse connections")
	}

	// B keeps serving and processing after A is gone.
	urlB, _ := mustCreateQueue(t, ctx, sqsB, "after-close-"+uniqueSuffix())

	if _, err := sqsB.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(urlB),
		MessageBody: aws.String("still-alive"),
	}); err != nil {
		t.Fatalf("SendMessage B after A closed: %v", err)
	}

	waitForMessage(t, ctx, sqsB, urlB, "still-alive")

	// Concurrent Close on B right after normal use must also be safe.
	for range 8 {
		wg.Add(1)

		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic during concurrent Close B: %v", r)
				}
			}()

			srvB.Close()
		}()
	}

	wg.Wait()
}

// TestMultiServer_CloseStopsBackgroundTasks verifies no per-instance
// background goroutines (notably the DynamoDB TTL reaper) survive Close.
func TestMultiServer_CloseStopsBackgroundTasks(t *testing.T) {
	var srvs []*kumo.Server

	for range 4 {
		srvs = append(srvs, kumo.NewServer())
	}

	for _, s := range srvs {
		s.Close()
	}

	// Give the reapers time to observe their stop channel.
	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		if goroutineStacksContain("ttlReaper") == 0 {
			return
		}

		time.Sleep(20 * time.Millisecond)
		runtime.GC()
	}

	t.Fatalf("dynamodb ttlReaper goroutine(s) still running after Close:\n%s", goroutineDump())
}

func goroutineStacksContain(substr string) int {
	n := 0

	for _, g := range strings.Split(goroutineDump(), "\n\n") {
		if strings.Contains(g, substr) {
			n++
		}
	}

	return n
}

func goroutineDump() string {
	buf := make([]byte, 1<<20)
	size := runtime.Stack(buf, true)

	return string(buf[:size])
}
