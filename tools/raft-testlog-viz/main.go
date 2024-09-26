// Convert a test log from a Raft run into an HTML file with a colorful
// table for easier tracking of the log events.
//
// Note: by "log" here we mean the logging messages emitted by our Raft code,
// not the raft log that stores replicated data.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"text/template"
)

// Entry is a single log entry emitted by a raft server.
type Entry struct {
	timestamp string
	id        string
	msg       string
}

// TestLog is a whole log for a single test, containing many entries.
type TestLog struct {
	name    string
	status  string
	entries []Entry

	// ids is a set of all IDs seen emitting entries in this test.
	ids map[string]bool
}

const tmpl = `
<!doctype html>
<html lang='en'>
  <head>
    <title>{{.Title}}</title>
  </head>
  <style>
  table {
    font-family: "Courier New";
    border-collapse: collapse;
  }

  table, th, td {
    padding: 8px;
    border: 1px solid #cccccc;
  }

  td.testcell {
    background-color: #ffffff;
  }

  td.Follower {
    background-color: #ffffff;
  }

  td.Candidate {
    background-color: #e2e2a3;
  }

  td.Leader {
    background-color: #e6fff5;
  }

  td.Dead {
    background-color: #dddddd;
  }

  h1 {
    text-align: center;
  }
  </style>
<body>
  <h1>{{.Title}}</h1>
  <p></p>
  <table>
    <tr>
      {{range .Headers}}
      <th>{{.}}</th>
      {{end}}
    </tr>
    {{range .HtmlItems}}
    <tr>
      {{.}}
    </tr>
    {{end}}
  </table>
</body>
</html>
`

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
	Dead
)

func (s ServerState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

func emitTestViz(dirname string, tl TestLog) {
	filename := path.Join(dirname, tl.name+".html")
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	t, err := template.New("page").Parse(tmpl)
	if err != nil {
		log.Fatal(err)
	}

	var nservers int
	var havetest bool

	if _, ok := tl.ids["TEST"]; ok {
		havetest = true
		nservers = len(tl.ids) - 1
	} else {
		havetest = false
		nservers = len(tl.ids)
	}

	headers := []string{"Time"}
	if havetest {
		headers = append(headers, "TEST")
	}
	for i := 0; i < nservers; i++ {
		headers = append(headers, strconv.Itoa(i))
	}

	serverState := make([]ServerState, nservers)

	var htmlitems []string
	for _, entry := range tl.entries {
		var b strings.Builder
		fmt.Fprintf(&b, "<td>%s</td>", entry.timestamp)
		if entry.id == "TEST" {
			if havetest {
				fmt.Fprintf(&b, `  <td class="testcell">%s</td>`, entry.msg)
				for i := 0; i < nservers; i++ {
					fmt.Fprintf(&b, `  <td class="%s"></td>`, serverState[i])
				}
			} else {
				log.Fatal("have TEST entry with no test IDs")
			}
		} else {
			idInt, err := strconv.Atoi(entry.id)
			if err != nil {
				log.Fatal(err)
			}

			if strings.Contains(entry.msg, "becomes Follower") {
				serverState[idInt] = Follower
			} else if strings.Contains(entry.msg, "listening") {
				serverState[idInt] = Follower
			} else if strings.Contains(entry.msg, "becomes Candidate") {
				serverState[idInt] = Candidate
			} else if strings.Contains(entry.msg, "becomes Leader") {
				serverState[idInt] = Leader
			} else if strings.Contains(entry.msg, "becomes Dead") {
				serverState[idInt] = Dead
			} else if strings.Contains(entry.msg, "created in state Follower") {
				serverState[idInt] = Follower
			}

			if havetest {
				fmt.Fprintf(&b, "  <td class=\"testcell\"></td>")
			}
			// Emit the right number of td's, with an entry in the right place.
			for i := 0; i < idInt; i++ {
				fmt.Fprintf(&b, `  <td class="%s"></td>`, serverState[i])
			}
			fmt.Fprintf(&b, `  <td class="%s">%s</td>`, serverState[idInt], entry.msg)
			for i := idInt + 1; i < nservers; i++ {
				fmt.Fprintf(&b, `  <td class="%s"></td>`, serverState[i])
			}
		}
		htmlitems = append(htmlitems, b.String())
	}

	data := struct {
		Title     string
		Headers   []string
		HtmlItems []string
	}{
		Title:     fmt.Sprintf("%s -- %s", tl.name, tl.status),
		Headers:   headers,
		HtmlItems: htmlitems,
	}
	err = t.Execute(f, data)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("... Emitted", "file://"+filename)
}

func parseTestLogs(rd io.Reader) []TestLog {
	var testlogs []TestLog

	statusRE := regexp.MustCompile(`--- (\w+):\s+(\w+)`)
	entryRE := regexp.MustCompile(`([0-9:.]+) \[([\w ]+)\] (.*)`)

	scanner := bufio.NewScanner(bufio.NewReader(rd))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "=== RUN") {
			testlogs = append(testlogs, TestLog{ids: make(map[string]bool)})
			testlogs[len(testlogs)-1].name = strings.TrimSpace(line[7:])
		} else {
			if len(testlogs) == 0 {
				continue
			}
			curlog := &testlogs[len(testlogs)-1]

			statusMatch := statusRE.FindStringSubmatch(line)
			if len(statusMatch) > 0 {
				if statusMatch[2] != curlog.name {
					log.Fatalf("name on line %q mismatch with test name: got %s", line, curlog.name)
				}
				curlog.status = statusMatch[1]
				continue
			}

			entryMatch := entryRE.FindStringSubmatch(line)
			if len(entryMatch) > 0 {
				// [kv N] entries get folded into id=N, with the "kv N" part prefixed
				// to the message.
				id, foundKV := strings.CutPrefix(entryMatch[2], "kv ")
				msg := entryMatch[3]
				if foundKV {
					msg = id + " " + msg
				}

				// [clientNNN] entries get folded into id=TEST
				if strings.HasPrefix(entryMatch[2], "client") {
					id = "TEST"
					msg = entryMatch[2] + " " + msg
				}

				entry := Entry{
					timestamp: entryMatch[1],
					id:        id,
					msg:       msg,
				}
				curlog.entries = append(curlog.entries, entry)
				curlog.ids[entry.id] = true
				continue
			}
		}
	}
	return testlogs
}

func main() {
	testlogs := parseTestLogs(os.Stdin)

	tnames := make(map[string]int)

	// Deduplicated the names of testlogs; in case the log containts multiple
	// instances of the same test, we'd like them all the have different file
	// names.
	for i, tl := range testlogs {
		if count, ok := tnames[tl.name]; ok {
			testlogs[i].name = fmt.Sprintf("%s_%d", tl.name, count)
		}
		tnames[tl.name] += 1
	}

	statusSummary := "PASS"

	for _, tl := range testlogs {
		fmt.Println(tl.status, tl.name, tl.ids, "; entries:", len(tl.entries))
		if tl.status != "PASS" {
			statusSummary = tl.status
		}
		emitTestViz("/tmp", tl)
		fmt.Println("")
	}

	fmt.Println(statusSummary)
}
