package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	pb "github.com/Anshul439/Orqestra/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func usage() {
	fmt.Println("usage:")
	fmt.Println("  go run ./cmd/cli submit [--type=<type>] [--payload=<json>] [--retries=<n>]")
	fmt.Println("  go run ./cmd/cli status <job-id>")
	fmt.Println("  go run ./cmd/cli list [--status=<status>]")
	fmt.Println("  go run ./cmd/cli cancel <job-id>")
	fmt.Println("  go run ./cmd/cli workflow list")
	fmt.Println("  go run ./cmd/cli workflow trigger <name>")
	fmt.Println("  go run ./cmd/cli workflow status <run-id>")
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

	case "workflow":
		if len(os.Args) < 3 {
			fmt.Println("error: missing workflow subcommand (list, trigger, status)")
			usage()
			os.Exit(1)
		}

		switch os.Args[2] {
		case "list":
			resp, err := client.ListWorkflows(context.Background(), &pb.ListWorkflowsRequest{})
			if err != nil {
				fmt.Println("error:", err)
				os.Exit(1)
			}
			for _, wf := range resp.Workflows {
				fmt.Printf("%-20s (%d steps)\n", wf.Name, wf.StepCount)
			}

		case "trigger":
			if len(os.Args) < 4 {
				fmt.Println("error: missing workflow name")
				os.Exit(1)
			}
			resp, err := client.TriggerWorkflow(context.Background(), &pb.TriggerWorkflowRequest{Name: os.Args[3]})
			if err != nil {
				fmt.Println("error:", err)
				os.Exit(1)
			}
			fmt.Printf("workflow triggered, run id: %d\n", resp.RunId)

		case "status":
			if len(os.Args) < 4 {
				fmt.Println("error: missing run id")
				os.Exit(1)
			}
			runID, err := strconv.Atoi(os.Args[3])
			if err != nil {
				fmt.Println("error: run id must be a number")
				os.Exit(1)
			}
			resp, err := client.GetWorkflowStatus(context.Background(), &pb.GetWorkflowStatusRequest{RunId: int32(runID)})
			if err != nil {
				fmt.Println("error:", err)
				os.Exit(1)
			}
			fmt.Printf("run %d (%s): status=%s step=%d/%d\n",
				resp.RunId, resp.WorkflowName, resp.Status, resp.CurrentStep, resp.TotalSteps)

		default:
			fmt.Printf("error: unknown workflow subcommand %q\n", os.Args[2])
			usage()
			os.Exit(1)
		}

	default:
		fmt.Printf("error: unknown command %q\n", os.Args[1])
		usage()
		os.Exit(1)
	}

}
