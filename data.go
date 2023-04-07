package main

type PaginationParams struct {
	PageSize int
	Offset   int
	Page     int
}

type ArticleData struct {
	JournalID   int
	Title       string
	Authornames []string
	Employers   []string
	KeyWords    []string
	Text        string
	Year        int
}

type JournalData struct {
	ID       int
	Title    string
	Articles []ArticleData
	Year     int
	Date     int64
}
