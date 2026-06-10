package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	pb "github.com/anshul439/go-orchestrator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func usage() {
	fmt.Println("usage:")
	fmt.Println("  go run ./cmd/cli submit [--type=<type>] [--payload=<json>] [--retries=<n>]")
	fmt.Println("  go run ./cmd/cli status <job-id>")
	fmt.Println("  go run ./cmd/cli list [--status=<status>]")
	fmt.Println("  go run ./cmd/cli cancel <job-id>")
}

func grpcTarget() string {
	target := os.Getenv("GRPC_ADDR")
	if target == "" {
		target = "localhost:50051"
	}

	// Server listen addresses often look like ":50051".
	// For the client, treat that as localhost on the same port.
	if strings.HasPrefix(target, ":") {
		return "localhost" + target
	}

	return target
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	conn, err := grpc.NewClient(
		grpcTarget(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Println("error creating gRPC client:", err)
		os.Exit(1)
	}

	defer conn.Close()

	client := pb.NewOrchestratorServiceClient(conn)

	switch os.Args[1] {
	case "submit":
		submitFlags := flag.NewFlagSet("submit", flag.ExitOnError)
		retries := submitFlags.Int("retries", 3, "max retry count")
		jobType := submitFlags.String("type", "generic", "job type")
		payload := submitFlags.String("payload", "{}", "job payload as JSON string")
		submitFlags.Parse(os.Args[2:])

		resp, err := client.SubmitJob(context.Background(), &pb.SubmitJobRequest{
			MaxRetries: int32(*retries),
			Type:       *jobType,
			Payload:    *payload,
		})

		if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}
		fmt.Printf("job submitted, id: %d (type=%s)\n", resp.JobId, *jobType)

	case "status":
		if len(os.Args) < 3 {
			fmt.Println("error: missing job id")
			usage()
			os.Exit(1)
		}

		id, err := strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Println("error: job id must be a number")
			os.Exit(1)
		}

		resp, err := client.GetJob(context.Background(), &pb.GetJobRequest{JobId: int32(id)})
		if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}
		fmt.Printf("job %d (%s): status=%s retries=%d/%d\n",
			resp.JobId, resp.Type, resp.Status, resp.RetryCount, resp.MaxRetries)

	case "list":
		listCmd := flag.NewFlagSet("list", flag.ExitOnError)
		status := listCmd.String("status", "", "filter by status (pending, running, completed, failed)")
		listCmd.Parse(os.Args[2:])

		resp, err := client.ListJobs(context.Background(), &pb.ListJobsRequest{Status: *status})
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}

		for _, j := range resp.Jobs {
			fmt.Printf("job %d (%s): status=%s retries=%d/%d\n",
				j.JobId, j.Type, j.Status, j.RetryCount, j.MaxRetries)
		}

	case "cancel":
		if len(os.Args) < 3 {
			fmt.Println("error: missing job id")
			usage()
			os.Exit(1)
		}

		id, err := strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Println("error: job id must be a number")
			os.Exit(1)
		}

		resp, err := client.CancelJob(context.Background(), &pb.CancelJobRequest{JobId: int32(id)})
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("job %d cancelled\n", resp.JobId)

	default:
		fmt.Printf("error: unknown command %q\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}
