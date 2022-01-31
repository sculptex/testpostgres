package main
 
import (
    "database/sql"
    "fmt"
    "strconv"
	"os"
	"log"
	
    "github.com/spf13/viper"
    
    _ "github.com/lib/pq"
)
 
const (
	KB = 1024
	MB = KB*KB
	GB = KB*MB
)

func showfilesize(bytes int64) string {
	if(bytes < KB) {
		return(fmt.Sprintf("%d bytes", bytes)) 
	}
	fbytes := float64(bytes)
	if(bytes < MB) {
		return(fmt.Sprintf("%0.1f KB", float64(fbytes/KB))) 
	}
	if(bytes < GB) {
		return(fmt.Sprintf("%0.2f MB", float64(fbytes/MB))) 
	}			
	return(fmt.Sprintf("%0.3f GB", float64(fbytes/GB))) 
}
 
func CheckError(err error) {
    if err != nil {
        // IGNORE ERRORS
        //panic(err)       
    }
}

func main() {

	viper.SetDefault("dbhost",		"172.21.0.3")    	
	viper.SetDefault("dbport",		"5432")    	
	viper.SetDefault("dbuser",		"postgres")    	
	viper.SetDefault("dbpassword",	"postgres")    	
	viper.SetDefault("dbname",		"blobber_meta")    	

	if len(os.Args) > 1 {
		configfile := os.Args[1]
		viper.SetConfigName(configfile)
		viper.AddConfigPath(".")
		viper.SetConfigType("yaml")
		viper.AutomaticEnv()
	    fmt.Printf("CONFIG: %s\n",configfile)	 
	    if err := viper.ReadInConfig(); err != nil {
	        if _, ok := err.(viper.ConfigFileNotFoundError); ok {
	            log.Println("no such config file")
	        } else {
	            log.Println("read config error")
	        }
	        log.Fatal (err)
	    }
	}    	
    	
	host := viper.GetString("dbhost")
	port := viper.GetString("dbport")
	user := viper.GetString("dbuser")
	password := viper.GetString("dbpassword")
	dbname := viper.GetString("dbname")

    fmt.Printf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable\n", host, port, user, password, dbname)         

    psqlconn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)         
    db, err := sql.Open("postgres", psqlconn)
    CheckError(err)
    defer db.Close()

    err = db.Ping()
    CheckError(err) 
    fmt.Println("Connected successfully!")

	res, err := db.Query("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'")
	CheckError(err)	 
	defer res.Close()
	for res.Next() {
	    var tablename string
	    err = res.Scan(&tablename)
	    CheckError(err)
	    
	    res2, err := db.Query("SELECT SUM(relpages) AS pagecount FROM pg_class WHERE relname = '"+tablename+"'")
		defer res2.Close()
		res2.Next()
	    var dbpagecount string
	    err = res2.Scan(&dbpagecount)
	    CheckError(err)

	    res3, err := db.Query("SELECT SUM(relpages) AS idxcount FROM pg_class WHERE relname <> '"+tablename+"' AND relname LIKE '%"+tablename+"%'")
		defer res3.Close()
		res3.Next()
	    var idxpagecount string
	    err = res3.Scan(&idxpagecount)
	    CheckError(err)

	    res4, err := db.Query("SELECT COUNT(*) AS rowcount FROM "+tablename)
		defer res4.Close()
		res4.Next()
	    var rowcount string
	    err = res4.Scan(&rowcount)
	    CheckError(err)

	    var rows int64
		rows , _ = strconv.ParseInt(rowcount, 10, 32)
	    
	    var dbbytes int64
	    var dbbytespr int
		dbbytes , _ = strconv.ParseInt(dbpagecount, 10, 64)
		dbbytes = dbbytes * 8 * 1024

	    var idxbytes int64
	    var idxbytespr int
		idxbytes , _ = strconv.ParseInt(idxpagecount, 10, 64)
		idxbytes = idxbytes * 8 * 1024
		
		if(rows > 0) {
			dbbytespr = int(dbbytes / rows)
			idxbytespr = int(idxbytes / rows)
		} else {
		    dbbytespr = 0
		    idxbytespr = 0
		}
	    
	    fmt.Printf("%-20s\t\t%d DB rows\t%s total\t(%s per row)\tIDX %s total\t(%s per row)\tTOTAL %s total\t(%s per row)\n", tablename, rows, showfilesize(dbbytes), showfilesize(int64(dbbytespr)), showfilesize(idxbytes), showfilesize(int64(idxbytespr)), showfilesize(dbbytes+idxbytes), showfilesize(int64(dbbytespr+idxbytespr)) )
	}

}
 
