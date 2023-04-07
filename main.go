package main

// Import package
import (
	"bufio"
	"fmt"
	"os"

	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
	//"Documents/go/reindexer/data"

	//"github.com/restream/reindexer/bindings/builtinserver/config"
	//"github.com/restream/reindexer/v3"
	// choose how the Reindexer binds to the app (in this case "builtin," which means link Reindexer as a static library)
	//_ "github.com/restream/reindexer/v3/bindings/builtin"
	// OR use Reindexer as standalone server and connect to it via TCP.

	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/v3/bindings/cproto"
	"gopkg.in/yaml.v2"
	// OR link Reindexer as static library with bundled server.
	// _ "github.com/restream/reindexer/v3/bindings/builtinserver"
)

// Define struct for config.yml and env files
type Config struct {
	Connection struct {
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"pass"`
		DBname   string `yaml:"dbname"`
	} `yaml:"db"`
}

// Define structures with reindex tags

type CRUD struct {
	Create string `reindex:"create"`
	Read   string `reindex:"read"`
	Update string `reindex:"update"`
	Delete string `reindex:"delete"`
}

type Article struct {
	ID          int      `reindex:"id,,pk"`
	JournalID   int      `reindex:"journalID"`
	Title       string   `reindex:"title"`
	Authornames []string `reindex:"authors"`
	Employers   []string `reindex:"employers"`
	KeyWords    []string `reindex:"keywords"`
	Text        string   `reindex:"text"`
	Year        int      `reindex:"year,tree"` // add sortable index by 'year' field
	Uploaded    string   `reindex:"uploadedAt"`
	Date        int64    `reindex:"date,ttl,,expire_after=900" json:"date"`
}

type Journal struct {
	ID       int           `reindex:"id,,pk"`    // 'id' is primary key
	Title    string        `reindex:"name"`      // add index by 'name' field
	Articles []ArticleData `reindex:"articles"`  // add index by articles 'articles' array
	Year     int           `reindex:"year,tree"` // add sortable index by 'year' field
	Date     int64         `reindex:"date,ttl,,expire_after=900" json:"date"`
}

type GeneralTable struct {
	ID          int64         `reindex:"id,,pk"`      // 'id' is primary key
	Journals    []JournalData `reindex:"articles"`    // add index by articles 'articles' array
	Description string        `reindex:"description"` // add sortable index by 'year' field
	Date        int64         `reindex:"date,ttl,,expire_after=3600" json:"date"`
}

func processError(err error) {
	fmt.Println(err)
	os.Exit(2)
}

func readFile(cfg *Config) {
	f, err := os.Open("config.yml")
	if err != nil {
		processError(err)
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(cfg)
	if err != nil {
		processError(err)
	}
}

func readEnv(cfg *Config) {
	err := envconfig.Process("", cfg)
	if err != nil {
		processError(err)
	}
}

// Read method. It allows users to query the list of documents
func GetDocumentsList(db *reindexer.Reindexer) {
	query := db.Query("journals").
		Sort("year", false).
		Limit(10).
		Offset(0).
		ReqTotal()

	iterator := query.Exec()

	defer iterator.Close()

	fmt.Println("Found", iterator.TotalCount(), "total documents, first", iterator.Count(), "documents:")

	var result []Journal

	for iterator.Next() {
		// Get the next document and cast it to a pointer
		elem := iterator.Object().(*Journal)
		//fmt.Println(*elem)
		result = append(result, *elem)
	}

	fmt.Println("The List of journals and their content: ", result)

	if err := iterator.Error(); err != nil {
		fmt.Println(err.Error())
	}

}

// Read method. This allows users to search for documents by author name and sort the result by year.
func GetDocumetsByJournalName(db *reindexer.Reindexer, journalname string) {
	query := db.Query("journals").
		Sort("year", true). // Sort results by 'year' field in ascending order
		Where("Title", reindexer.LIKE, "%"+journalname+"%").
		Limit(10). // Return maximum 10 documents
		Offset(0). // from 0 position
		ReqTotal() // Calculate the total count of matching documents

	// Execute the query and return an iterator
	iterator := query.Exec()
	// Iterator must be closed
	defer iterator.Close()

	fmt.Println("Found", iterator.TotalCount(), "total documents, first", iterator.Count(), "documents:")

	// Iterate over results
	for iterator.Next() {
		// Get the next document and cast it to a pointer
		elem := iterator.Object().(*Journal)
		fmt.Println(*elem)
	}
	// Check the error
	if err := iterator.Error(); err != nil {
		fmt.Println(err.Error())
	}
}

// Create method
func AddArticle(db *reindexer.Reindexer, a *ArticleData) {
	//  method "Create"
	err := db.Upsert("articles", &Article{
		Title:       a.Title,
		Authornames: a.Authornames,
		Employers:   a.Employers,
		KeyWords:    a.KeyWords,
		Text:        a.Text,
		Year:        a.Year,
		Uploaded:    string(time.Now().GoString()),
		Date:        time.Now().Unix(),
	}, "id=serial()")
	if err != nil {
		fmt.Println(err.Error())
	}
}

func AddJournal(db *reindexer.Reindexer, b *JournalData) {
	//  method "Create"
	err := db.Upsert("journals", &Journal{
		Title:    b.Title,
		Articles: b.Articles,
		Year:     b.Year,
		Date:     time.Now().Unix(),
	}, "id=serial()")
	if err != nil {
		fmt.Println(err.Error())
	}
}

func Update(db *reindexer.Reindexer, title string, values string) {
	err := db.Query("journals").Where("Title", reindexer.LIKE, "%"+title+"%").Set("Year", values).Update()
	fmt.Println(err.Error())
}

func Delete(db *reindexer.Reindexer, tablename string) {
	err := db.TruncateNamespace(tablename)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func DeleteTabel(db *reindexer.Reindexer, tablename string) {
	err := db.DropNamespace(tablename)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func main() {
	answer := true
	var cfg Config
	readFile(&cfg)
	readEnv(&cfg)
	fmt.Printf("%+v\n", cfg)

	article1 := &ArticleData{
		JournalID:   1,
		Title:       "Building an Historical CRUD",
		Authornames: []string{"Dino Esposito"},
		Employers:   []string{"Software Architect & Digital Strategist"},
		KeyWords:    []string{"Create", "Read", "Update", "Delete"},
		Text:        "Relational databases have been around since the 1970s and a few generations of developers started and ended their careers without learning, or just mildly considering, an alternate approach to data storage. Recently, large social networks provided strong evidence that relational databases couldn`t serve all possible business scenarios. When a (really) huge amount of schemaless data comes your way, relational databases might sometimes be a bottleneck rather than a pipe....",
		Year:        2016,
	}

	article2 := &ArticleData{
		JournalID:   1,
		Title:       "CRUD Operations - What is CRUD?",
		Authornames: []string{"Kolade Chris"},
		Employers:   []string{"Freelancer"},
		KeyWords:    []string{"Create", "Read", "Update", "Delete"},
		Text:        "In this article, I will show you what CRUD means, and what the individual terms mean and do. I will also show you how create, read, update, and delete operations work in the real world.",
		Year:        2022,
	}

	article3 := &ArticleData{
		JournalID:   1,
		Title:       "What is a CRUD app and how to build one | Ultimate guide",
		Authornames: []string{"Joe Johnston"},
		Employers:   []string{"Freelancer"},
		KeyWords:    []string{"CRUD", "app"},
		Text:        "We use CRUD apps every day. Most of the time, without noticing. They keep us organized, they help digitise business processes, and they’re critical to application development. But many of us are oblivious to what CRUD apps are, or how to build one.",
		Year:        2021,
	}

	journal1 := &JournalData{
		ID:       1,
		Title:    "Trending Modern computer Technologies",
		Articles: []ArticleData{*article1, *article2, *article3},
		Year:     2022,
	}

	journal2 := &JournalData{
		ID:       1,
		Title:    "Computers",
		Articles: []ArticleData{*article2, *article3},
		Year:     2020,
	}

	journal3 := &JournalData{
		ID:       1,
		Title:    "IT industry",
		Articles: []ArticleData{*article1, *article2},
		Year:     1996,
	}

	// Init a database instance and choose the binding (builtin)
	// db := reindexer.NewReindex("builtin:///tmp/reindex/testdb")

	// OR - Init a database instance and choose the binding (connect to server)
	// Database should be created explicitly via reindexer_tool or via WithCreateDBIfMissing option:
	// If server security mode is enabled, then username and password are mandatory
	db_connector := "cproto://" + cfg.Connection.User + ":" + cfg.Connection.Password + "@" + cfg.Connection.Host + ":" + cfg.Connection.Port + "/" + cfg.Connection.DBname
	db := reindexer.NewReindex(db_connector, reindexer.WithCreateDBIfMissing())
	//db := reindexer.NewReindex(`cproto://user:pass@127.0.0.1:6534/testdb`, reindexer.WithCreateDBIfMissing())

	// Check if DB was initialized correctly and testing the connection
	if db.Status().Err != nil {
		panic(db.Status().Err)
	}
	defer db.Close()

	/* Or we can setup this checker of db connection health
	if db.Status().Err == nil {
		println("A connection was successfully established with the Reindexer server!")
	} else {
		fmt.Println("Cannot connect to the Reindexer server")
		os.Exit(0)
	}
	*/

	// OR - Init a database instance and choose the binding (builtin, with bundled server)
	///serverConfig := config.DefaultServerConfig()
	// If server security mode is enabled, then username and password are mandatory
	//db := reindexer.NewReindex("builtinserver://user:pass@testdb", reindexer.WithServerConfig(100*time.Second, serverConfig))

	//  Open or create new namespace and indexes based on passed struct. IndexDef fields of struct are marked by
	err := db.OpenNamespace("articles", reindexer.DefaultNamespaceOptions(), Article{})

	if err != nil {
		fmt.Println("Table 'articles' was not opened!")
		fmt.Println(err.Error())
	} else {
		fmt.Println("Table 'articles' was opened!")
	}

	err = db.OpenNamespace("journals", reindexer.DefaultNamespaceOptions(), Journal{})

	if err != nil {
		fmt.Println("Table 'journals' was not opened!")
		fmt.Println(err.Error())
	} else {
		fmt.Println("Table 'journals' was opened!")
	}

	err = db.OpenNamespace("generaltable", reindexer.DefaultNamespaceOptions(), GeneralTable{})

	if err != nil {
		fmt.Println("Table 'generaltable' was not opened!")
		fmt.Println(err.Error())
	} else {
		fmt.Println("Table 'generaltable' was opened!")
	}

	for answer {
		fmt.Println("Do you want to make a request[Y/N]?")
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')

		if string(input) == "N\n" {
			os.Exit(0)
		}
		if string(input) == "Y\n" {
			var tablename, title, year string
			var n int
			fmt.Println("Choose options(for instance \"1\"): ")
			fmt.Println("1 - Get the list of journals")
			fmt.Println("2 - Find a journal by its name")
			fmt.Println("3 - Add new documents")
			fmt.Println("4 - Update journal information")
			fmt.Println("5 - Purge a table")
			fmt.Println("6 - Delete a table")
			fmt.Scanln(&n)

			switch n {
			case 1:
				fmt.Println("Searching for journals...")
				GetDocumentsList(db)

			case 2:
				fmt.Println("Enter the title of the journal you are looking for: ")
				reader := bufio.NewReader(os.Stdin)
				journalname, _ := reader.ReadString('\n')
				journalname = strings.TrimSuffix(journalname, "\n")
				GetDocumetsByJournalName(db, journalname)
			case 3:
				AddArticle(db, article1)
				AddArticle(db, article2)
				AddArticle(db, article3)
				AddJournal(db, journal1)
				AddJournal(db, journal2)
				AddJournal(db, journal3)

			case 4:
				fmt.Println("Enter the title of the journal you want to update:")
				fmt.Scanln(&title)
				fmt.Println("Specify the new value for the field \"year\" you want to enter:")
				fmt.Scanln(&year)
				Update(db, title, year)
			case 5:
				fmt.Println("Enter the table name you want to purge")
				fmt.Scanln(&tablename)
				Delete(db, tablename)
			case 6:
				fmt.Println("Enter the table you want to delete")
				fmt.Scanln(&tablename)
				DeleteTabel(db, tablename)
			}
		} else {
			println("You need to choose \"Y\" or \"N\"")
		}
	}
}
