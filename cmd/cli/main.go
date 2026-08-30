package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func baseURL() string {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = "localhost:8080"
	}
	// Strip leading colon so ":8080" becomes "localhost:8080".
	if strings.HasPrefix(addr, ":") {
		addr = "localhost" + addr
	}
	return "http://" + addr
}

func apiKey() string {
	return os.Getenv("ORQESTRA_API_KEY")
}

func usage() {
	fmt.Println("usage:")
	fmt.Println("  orq submit [--type=<type>] [--command=<shell>] [--payload=<json>] [--retries=<n>]")
	fmt.Println("  orq status <job-id>")
	fmt.Println("  orq list [--status=<status>]")
	fmt.Println("  orq cancel <job-id>")
	fmt.Println("  orq workflow list")
	fmt.Println("  orq workflow trigger <name>")
	fmt.Println("  orq workflow runs")
	fmt.Println("  orq workflow status <run-id>")
	fmt.Println("  orq workflow cancel <run-id>")
}

func doJSON(method, url string, body any) (map[string]any, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		r = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key := apiKey(); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		if msg, ok := result["error"].(string); ok {
			return nil, fmt.Errorf("%s", msg)
		}
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	return result, nil
}

func doJSONArray(method, url string) ([]any, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}
	if key := apiKey(); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result []any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	return result, nil
}

func intField(m map[string]any, key string) int {
	v, _ := m[key].(float64)
	return int(v)
}

func strField(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	base := baseURL()

	switch os.Args[1] {
	case "submit":
		submitFlags := flag.NewFlagSet("submit", flag.ExitOnError)
		retries := submitFlags.Int("retries", 3, "max retry count")
		jobType := submitFlags.String("type", "generic", "job type")
		command := submitFlags.String("command", "", "shell command to execute")
		payload := submitFlags.String("payload", "{}", "job payload as JSON string")
		submitFlags.Parse(os.Args[2:])

		if *command != "" {
			cmd := strings.Trim(*command, "'")
			b, err := json.Marshal(struct {
				Command string `json:"command"`
			}{Command: cmd})
			if err != nil {
				fmt.Println("error marshaling command payload:", err)
				os.Exit(1)
			}
			*payload = string(b)
			if *jobType == "generic" {
				*jobType = "shell"
			}
		}

		result, err := doJSON(http.MethodPost, base+"/api/v1/jobs", map[string]any{
			"type":        *jobType,
			"payload":     *payload,
			"max_retries": *retries,
		})
		if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}
		fmt.Printf("job submitted, id: %d (type=%s)\n", intField(result, "job_id"), *jobType)

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

		result, err := doJSON(http.MethodGet, fmt.Sprintf("%s/api/v1/jobs/%d", base, id), nil)
		if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}
		fmt.Printf("job %d (%s): status=%s retries=%d/%d\n",
			intField(result, "id"),
			strField(result, "type"),
			strField(result, "status"),
			intField(result, "retry_count"),
			intField(result, "max_retries"),
		)
		if out := strField(result, "output"); out != "" {
			fmt.Printf("output:\n%s\n", out)
		}

	case "list":
		listCmd := flag.NewFlagSet("list", flag.ExitOnError)
		status := listCmd.String("status", "", "filter by status")
		listCmd.Parse(os.Args[2:])

		url := base + "/api/v1/jobs"
		if *status != "" {
			url += "?status=" + *status
		}

		jobs, err := doJSONArray(http.MethodGet, url)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		for _, raw := range jobs {
			j, _ := raw.(map[string]any)
			fmt.Printf("job %d (%s): status=%s retries=%d/%d\n",
				intField(j, "id"),
				strField(j, "type"),
				strField(j, "status"),
				intField(j, "retry_count"),
				intField(j, "max_retries"),
			)
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

		_, err = doJSON(http.MethodPost, fmt.Sprintf("%s/api/v1/jobs/%d/cancel", base, id), nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		fmt.Printf("job %d cancelled\n", id)

	case "workflow":
		if len(os.Args) < 3 {
			fmt.Println("error: missing workflow subcommand (list, trigger, status)")
			usage()
			os.Exit(1)
		}

		switch os.Args[2] {
		case "list":
			wfs, err := doJSONArray(http.MethodGet, base+"/api/v1/workflows")
			if err != nil {
				fmt.Println("error:", err)
				os.Exit(1)
			}
			for _, raw := range wfs {
				wf, _ := raw.(map[string]any)
				line := fmt.Sprintf("%-20s (%d steps)", strField(wf, "name"), intField(wf, "step_count"))
				if s := strField(wf, "schedule"); s != "" {
					line += fmt.Sprintf(", schedule: %s", s)
				}
				fmt.Println(line)
			}

		case "trigger":
			if len(os.Args) < 4 {
				fmt.Println("error: missing workflow name")
				os.Exit(1)
			}
			name := os.Args[3]
			result, err := doJSON(http.MethodPost, fmt.Sprintf("%s/api/v1/workflows/%s/trigger", base, name), nil)
			if err != nil {
				fmt.Println("error:", err)
				os.Exit(1)
			}
			fmt.Printf("workflow triggered, run id: %d\n", intField(result, "run_id"))

		case "runs":
			runs, err := doJSONArray(http.MethodGet, base+"/api/v1/workflows/runs")
			if err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
				os.Exit(1)
			}
			for _, raw := range runs {
				r, _ := raw.(map[string]any)
				fmt.Printf("run %d (%s): status=%s step=%d/%d\n",
					intField(r, "id"),
					strField(r, "workflow_name"),
					strField(r, "status"),
					intField(r, "current_step"),
					intField(r, "total_steps"),
				)
			}

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
			result, err := doJSON(http.MethodGet, fmt.Sprintf("%s/api/v1/workflows/runs/%d", base, runID), nil)
			if err != nil {
				fmt.Println("error:", err)
				os.Exit(1)
			}
			fmt.Printf("run %d (%s): status=%s step=%d/%d\n",
				intField(result, "id"),
				strField(result, "workflow_name"),
				strField(result, "status"),
				intField(result, "current_step"),
				intField(result, "total_steps"),
			)

		case "cancel":
			if len(os.Args) < 4 {
				fmt.Println("error: missing run id")
				os.Exit(1)
			}
			runID, err := strconv.Atoi(os.Args[3])
			if err != nil {
				fmt.Println("error: run id must be a number")
				os.Exit(1)
			}
			_, err = doJSON(http.MethodPost, fmt.Sprintf("%s/api/v1/workflows/runs/%d/cancel", base, runID), nil)
			if err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
				os.Exit(1)
			}
			fmt.Printf("workflow run %d cancelled\n", runID)

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
