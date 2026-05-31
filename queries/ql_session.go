package queries

import (
	"strings"

	table "github.com/MHS-20/ElkDB/tables"
)

type StmtSplitter struct {
	buffer string
}

func (s *StmtSplitter) Feed(chunk string) { s.buffer += chunk }
func (s *StmtSplitter) HasPending() bool  { return s.buffer != "" }

func (s *StmtSplitter) Pop() (string, bool) {
	var inString bool
	for i := 0; i < len(s.buffer); i++ {
		ch := s.buffer[i]
		if ch == '\'' {
			inString = !inString
			continue
		}
		if ch == ';' && !inString {
			stmt := s.buffer[:i+1]
			s.buffer = s.buffer[i+1:]
			return stmt, true
		}
	}
	return "", false
}

type Session struct {
	DB       table.DB
	splitter StmtSplitter
}

func NewSession(path string) (*Session, error) {
	s := &Session{}
	s.DB.Path = path
	if err := s.DB.Open(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Session) Close() { s.DB.Close() }

// ExecChunk feeds a chunk of text, executes any complete statements,
// and returns their results (or the first error encountered).
func (s *Session) ExecChunk(chunk string) ([]Result, error) {
	s.splitter.Feed(chunk)
	var results []Result
	for {
		rawStmt, ok := s.splitter.Pop()
		if !ok {
			break
		}
		stmt := strings.TrimSuffix(strings.TrimSpace(rawStmt), ";")

		tx := table.DBTX{}
		s.DB.Begin(&tx)

		var result Result
		var err error
		if strings.HasPrefix(strings.ToUpper(stmt), "SELECT") {
			result, err = ReaderExecString(&tx, stmt)
		} else {
			result, err = WriterExecString(&tx, stmt)
		}
		if err != nil {
			s.DB.Abort(&tx)
			return results, err // return partial results + the error
		}
		if err := s.DB.Commit(&tx); err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}
