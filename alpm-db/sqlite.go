package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func moveFile(sourcePath, destPath string) error {
	// canèt use "rename" in /tmp
	inputFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("Couldn't open source file: %s", err)
	}
	outputFile, err := os.Create(destPath)
	if err != nil {
		inputFile.Close()
		return fmt.Errorf("Couldn't open dest file: %s", err)
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, inputFile)
	inputFile.Close()
	if err != nil {
		return fmt.Errorf("Writing to output file failed: %s", err)
	}
	// The copy was successful, so now delete the original file
	err = os.Remove(sourcePath)
	if err != nil {
		return fmt.Errorf("Failed removing original file: %s", err)
	}
	return nil
}

func getSepDepend(dep string) string {
	seps := []string{">=", "<=", "=", "<", ">"}
	for _, sep := range seps {
		if strings.Contains(dep, sep) {
			return sep
		}
	}
	return ""
}

func GenSqlite(pkgs Packages) {
	fmt.Println("\n", COLOR_BLUE, "--- sqlite génération...", COLOR_NONE)
	os.Remove("./pacman.db")
	os.Remove("/tmp/pacman.db")
	//defer os.Rename("/tmp/pacman.db", "./pacman.db")
	db, err := sql.Open("sqlite3", "/tmp/pacman.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tstart := time.Now() // start timer
	/* pkgs: UNIQUE(name)
	 * ignore duplicate as pacman (by order of repos)
	 */
	stmt, _ := db.Prepare("CREATE TABLE IF NOT EXISTS pkgs (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, base TEXT DEFAULT NULL, version TEXT NOT NULL, repo INTEGER, desc TEXT, url TEXT, builddate TIME, csize INTEGER, isize INTEGER, packager INTEGER);")
	stmt.Exec()
	stmt, _ = db.Prepare("CREATE TABLE IF NOT EXISTS depends (id INTEGER, depend TEXT, comp TEXT, ver TEXT, pkg INTEGER DEFAULT -1)")
	stmt.Exec()
	stmt, _ = db.Prepare("CREATE TABLE IF NOT EXISTS optdepends (id INTEGER, optdepend TEXT, pkg INTEGER DEFAULT -1)")
	stmt.Exec()
	stmt, _ = db.Prepare("CREATE TABLE IF NOT EXISTS provides (id INTEGER, provide TEXT, comp TEXT, ver TEXT, pkg INTEGER DEFAULT -1)")
	stmt.Exec()
	stmt, _ = db.Prepare("CREATE TABLE IF NOT EXISTS conflicts (id INTEGER, conflict TEXT, comp TEXT, ver TEXT, pkg INTEGER DEFAULT -1)")
	stmt.Exec()
	stmt, _ = db.Prepare("CREATE TABLE IF NOT EXISTS makedepends (id INTEGER, depend TEXT, comp TEXT, ver TEXT, pkg INTEGER DEFAULT -1)")
	stmt.Exec()
	stmt, _ = db.Prepare("CREATE TABLE IF NOT EXISTS licences (id INTEGER, licence TEXT)")
	stmt.Exec()
	stmt, _ = db.Prepare("CREATE TABLE IF NOT EXISTS packagers (id INTEGER PRIMARY KEY, packager TEXT UNIQUE)")
	stmt.Exec()
	stmt, _ = db.Prepare("CREATE TABLE IF NOT EXISTS repos (id INTEGER PRIMARY KEY, repo TEXT UNIQUE)")
	stmt.Exec()

	fmt.Println("packagers table ...")
	j := 1
	for i, pkg := range pkgs {
		if len(pkg.PACKAGER) < 1 {
			continue
		}
		stmt, _ := db.Prepare("INSERT or IGNORE INTO packagers (id,packager) VALUES (?, ?)")
		ret, _ := stmt.Exec(j, pkg.PACKAGER)
		if ret != nil {
			nb, _ := ret.RowsAffected()
			if nb > 0 {
				pkgs[i].PACKAGER = strconv.FormatInt(int64(j), 10)
				j = j + 1
			} else {
				var j int64
				err = db.QueryRow("SELECT id FROM packagers WHERE packager='" + pkg.PACKAGER + "' LIMIT 1").Scan(&j)
				if err != nil {
					log.Fatal(err)
				}
				pkgs[i].PACKAGER = strconv.FormatInt(int64(j), 10)
			}
		}
	}

	fmt.Println("repos table ...")
	j = 1
	for i, pkg := range pkgs {
		if len(pkg.REPO) < 1 {
			continue
		}
		stmt, _ := db.Prepare("INSERT or IGNORE INTO repos (id,repo) VALUES (?, ?)")
		ret, _ := stmt.Exec(j, pkg.REPO)
		if ret != nil {
			nb, _ := ret.RowsAffected()
			if nb > 0 {
				pkgs[i].REPO = strconv.FormatInt(int64(j), 10)
				j = j + 1
			} else {
				var j int64
				err = db.QueryRow("SELECT id FROM repos WHERE repo='" + pkg.REPO + "' LIMIT 1").Scan(&j)
				if err != nil {
					log.Fatal(err)
				}
				pkgs[i].REPO = strconv.FormatInt(int64(j), 10)
			}
		}
	}

	fmt.Println("main table ...")
	sqlStr := "INSERT or IGNORE INTO pkgs (id, name, base, version, repo, url, desc, builddate, csize, isize, packager) VALUES "
	vals := []interface{}{}
	for i, pkg := range pkgs {
		sqlStr += "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),"
		t := time.Unix(pkg.BUILDDATE, 0)
		vals = append(vals, pkg.id, pkg.NAME, pkg.getBase(), pkg.VERSION, pkg.REPO, pkg.URL, pkg.DESC, t.Format("2006-02-01 15:04:05"), pkg.CSIZE, pkg.ISIZE, pkg.PACKAGER)
		if i%50 == 0 {
			sqlStr = strings.TrimSuffix(sqlStr, ",")
			stmt, err := db.Prepare(sqlStr)
			if err != nil {
				fmt.Println("Error Prepare", sqlStr)
				log.Fatal(err)
			}
			//fmt.Print(i, "... ")
			_, err = stmt.Exec(vals...)
			if err != nil {
				//fmt.Println("Error insert ", i, sqlStr)
				//fmt.Println("Error insert ", i, vals)
				log.Fatal(err)
			}
			sqlStr = "INSERT or IGNORE INTO pkgs (id, name, base, version, repo, url, desc, builddate, csize, isize, packager) VALUES "
			vals = []interface{}{}
		}
	}
	if len(vals) > 0 {
		sqlStr = strings.TrimSuffix(sqlStr, ",")
		stmt, err := db.Prepare(sqlStr)
		if err != nil {
			fmt.Println("Error insert Prepare", sqlStr)
			log.Fatal(err)
		}
		//fmt.Println("rest ...")
		stmt.Exec(vals...)
	}

	fmt.Println("depends table ...")
	for _, pkg := range pkgs {
		if len(pkg.DEPENDS) < 1 {
			continue
		}

		sqlStr := "INSERT INTO depends (id, depend, comp, ver, pkg) VALUES "
		vals := []interface{}{}
		for _, dep := range pkg.DEPENDS {
			dep = strings.TrimSpace(dep)
			ver := ""
			comp := getSepDepend(dep)
			if comp != "" {
				tmp := strings.SplitN(dep, comp, 2)
				ver = tmp[1]
				dep = tmp[0]
			}
			sqlStr += "(?, ?, ?, ?, ?),"
			// TODO ? dep = "" if pkgs.FindByName(dep).Valid
			vals = append(vals, pkg.id, dep, comp, ver, pkgs.FindByName(dep))
		}
		sqlStr = strings.TrimSuffix(sqlStr, ",")
		stmt, _ := db.Prepare(sqlStr)
		_, err = stmt.Exec(vals...)
		if err != nil {
			fmt.Println("Error depends insert", sqlStr, vals)
			log.Fatal(err)
		}
	}

	fmt.Println("optional depends table ...")
	for _, pkg := range pkgs {
		if len(pkg.OPTDEPENDS) < 1 {
			continue
		}

		sqlStr := "INSERT INTO optdepends (id, optdepend, pkg) VALUES "
		vals := []interface{}{}
		for _, dep := range pkg.OPTDEPENDS {
			dep = strings.SplitN(dep, ":", 2)[0]
			dep = strings.TrimSpace(dep)
			comp := getSepDepend(dep)
			if comp != "" {
				dep = strings.SplitN(dep, comp, 2)[0]
			}
			sqlStr += "(?, ?, ?),"
			// TODO ? dep = "" if pkgs.FindByName(dep).Valid
			vals = append(vals, pkg.id, dep, pkgs.FindByName(dep))
		}
		sqlStr = strings.TrimSuffix(sqlStr, ",")
		stmt, _ := db.Prepare(sqlStr)
		_, err = stmt.Exec(vals...)
		if err != nil {
			fmt.Println("Error optional depends insert", sqlStr, vals)
			log.Fatal(err)
		}
	}

	fmt.Println("conflits table ...")
	for _, pkg := range pkgs {
		if len(pkg.CONFLICTS) < 1 {
			continue
		}

		sqlStr := "INSERT INTO conflicts (id, conflict, comp, ver, pkg) VALUES "
		vals := []interface{}{}
		for _, dep := range pkg.CONFLICTS {
			dep = strings.TrimSpace(dep)
			ver := ""
			comp := getSepDepend(dep)
			if comp != "" {
				tmp := strings.SplitN(dep, comp, 2)
				ver = tmp[1]
				dep = tmp[0]
			}
			sqlStr += "(?, ?, ?, ?, ?),"
			vals = append(vals, pkg.id, dep, comp, ver, pkgs.FindByName(dep))
		}
		sqlStr = strings.TrimSuffix(sqlStr, ",")
		stmt, _ := db.Prepare(sqlStr)
		_, err = stmt.Exec(vals...)
		if err != nil {
			fmt.Println("Error conflicts insert", sqlStr, vals)
			log.Fatal(err)
		}
	}

	fmt.Println("provides table ...")
	for _, pkg := range pkgs {
		if len(pkg.PROVIDES) < 1 {
			continue
		}

		sqlStr := "INSERT INTO provides (id, provide, comp, ver, pkg) VALUES "
		vals := []interface{}{}
		for _, dep := range pkg.PROVIDES {
			dep = strings.TrimSpace(dep)
			ver := ""
			comp := getSepDepend(dep)
			if comp != "" {
				tmp := strings.SplitN(dep, comp, 2)
				ver = tmp[1]
				dep = tmp[0]
			}
			sqlStr += "(?, ?, ?, ?, ?),"
			vals = append(vals, pkg.id, dep, comp, ver, pkgs.FindByName(dep))
		}
		sqlStr = strings.TrimSuffix(sqlStr, ",")
		stmt, _ := db.Prepare(sqlStr)
		_, err = stmt.Exec(vals...)
		if err != nil {
			fmt.Println("Error provides insert", sqlStr, vals)
			log.Fatal(err)
		}
	}

	fmt.Println("licences table ...")
	for _, pkg := range pkgs {
		if len(pkg.LICENSE) < 1 {
			continue
		}

		sqlStr := "INSERT INTO licences (id, licence) VALUES "
		vals := []interface{}{}
		for _, dep := range pkg.LICENSE {
			sqlStr += "(?, ?),"
			vals = append(vals, pkg.id, strings.TrimSpace(dep))
		}
		sqlStr = strings.TrimSuffix(sqlStr, ",")
		stmt, _ := db.Prepare(sqlStr)
		_, err = stmt.Exec(vals...)
		if err != nil {
			fmt.Println("Error licences insert", sqlStr, vals)
			log.Fatal(err)
		}
	}

	for _, pkg := range pkgs {
		if len(pkg.MAKEDEPENDS) < 1 {
			continue
		}
		sqlStr := "INSERT INTO makedepends (id, depend, comp, ver, pkg) VALUES "
		vals := []interface{}{}
		for _, dep := range pkg.MAKEDEPENDS {
			dep = strings.TrimSpace(dep)
			comp := ""
			if strings.Contains(dep, ">=") {
				comp = ">="
			} else {
				if strings.Contains(dep, "=") {
					comp = "="
				}
			}
			ver := ""
			if len(comp) > 0 {
				tmp := strings.SplitN(dep, "=", 2)
				ver = tmp[1]
				dep = tmp[0]
				dep = strings.TrimSuffix(dep, ">")

			}
			sqlStr += "(?, ?, ?, ?, ?),"
			vals = append(vals, pkg.id, dep, comp, ver, pkgs.FindByName(dep))
		}
		sqlStr = strings.TrimSuffix(sqlStr, ",")
		stmt, _ := db.Prepare(sqlStr)
		_, err = stmt.Exec(vals...)
		if err != nil {
			fmt.Println("Error makedepends insert", sqlStr)
			log.Fatal(err)
		}
	}

	fmt.Println("create index...")
	stmt, _ = db.Prepare("CREATE INDEX index_repo ON pkgs (repo ASC);")
	stmt.Exec()
	stmt, _ = db.Prepare("CREATE INDEX index_name ON pkgs (name ASC);")
	stmt.Exec()

	/*
		// replaced by pkgs.FindByName(dep)
		fmt.Println("JOIN depends to pkgs...")
		stmt, _ = db.Prepare("update depends set pkg=(select id from pkgs where depends.depend LIKE pkgs.name LIMIT 1)")
		_, err = stmt.Exec()
		if err != nil {
			fmt.Println("Error depends link depend name to package id", sqlStr)
			log.Fatal(err)
		}
		fmt.Println("JOIN makedepends to pkgs...")
		stmt, _ = db.Prepare("update makedepends set pkg=(select id from pkgs where makedepends.depend LIKE pkgs.name LIMIT 1)")
		_, err = stmt.Exec()
		if err != nil {
			fmt.Println("Error makedepends link depend name to package id", sqlStr)
			log.Fatal(err)
		}
	*/

	telapsed := time.Since(tstart)
	fmt.Println("\nsql duration: ", COLOR_GREEN, telapsed, COLOR_NONE, "\n ")

	fmt.Println("\n---\nTests...")
	var nb int64
	// SELECT count(DISTINCT id)
	err = db.QueryRow("SELECT count(id) AS nb FROM pkgs").Scan(&nb)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(nb, " pkgs in Database sql ")
	/*
		select name,builddate from pkgs where strftime('%Y',builddate)='2013'

		SELECT pkgs.id, name, depend  FROM pkgs LEFT JOIN depends ON pkgs.id=depends.id WHERE depend='gtk2' order by name
		SELECT pkgs.id, name, depend  FROM pkgs LEFT JOIN makedepends ON pkgs.id=makedepends.id WHERE depend='gtk2' order by name

		SELECT *  FROM packagers WHERE packager LIKE '%manjaro%' order by packager
		SELECT name, packagers.packager FROM pkgs LEFT JOIN packagers ON pkgs.packager=packagers.id WHERE packagers.packager LIKE '%manjaro%' order by packagers.packager

		SELECT count(name) as "count", packagers.packager, packagers.id FROM pkgs LEFT JOIN packagers ON pkgs.packager=packagers.id GROUP BY packagers.id HAVING packagers.packager LIKE '%manjaro%' order by "count" DESC
	*/

	dir, _ := os.Getwd()
	err = moveFile("/tmp/pacman.db", dir+"/pacman.db")
	if err != nil {
		log.Fatal(err)
	}
}

/*
./ls-alpm -q "select * from repos;"
OR
sqlite3 pacman.db "select * from repos;"
*/
func RequestSql(requestStr string) {
	if len(requestStr) < 5 {
		return
	}
	db, err := sql.Open("sqlite3", "pacman.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("::", COLOR_BLUE, requestStr, COLOR_NONE)
	rows, err := db.Query(requestStr)
	if err != nil {
		panic(err)
	}
	cols, _ := rows.Columns()
	fmt.Println("--------------------" + COLOR_GREEN)
	fmt.Printf(strings.Join(cols, "\t"+COLOR_NONE+"|\t"+COLOR_GREEN))
	fmt.Println(COLOR_NONE, "\n--------------------")
	defer rows.Close()

	for rows.Next() {
		columns := make([]string, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}

		err := rows.Scan(columnPointers...)
		if err != nil {
			fmt.Println("Error: ", err)
		}
		fmt.Printf(strings.Join(columns, "\t|\t") + "\n")
	}
	fmt.Println("")
	/*
		./ls-alpm -q "select * from repos"
		./ls-alpm -q "SELECT count(name) as "count", packagers.packager, packagers.id FROM pkgs LEFT JOIN packagers ON pkgs.packager=packagers.id GROUP BY packagers.id HAVING packagers.packager LIKE '%manjaro%' order by 'count' DESC"
	*/
}

func RunSql(title string, requestStr string) {
	var out bytes.Buffer
	var err bytes.Buffer

	cmd := exec.Command("sqlite3", "-readonly", "pacman.db", ""+requestStr+";")
	if !strings.Contains(requestStr, "json_object") {
		// NOT ./ls-alpm -q "select json_object('id',id,'repo',repo) from repos"
		cmd = exec.Command("sqlite3", "-readonly", "pacman.db", ""+requestStr+";", "-cmd", ".header on", "-cmd", ".mode column")
		if len(title) < 1 {
			title = requestStr
		}
		fmt.Println("::", COLOR_GREEN, title, COLOR_NONE)
	}
	cmd.Stdout = &out
	cmd.Stderr = &err
	cmd.Run()
	if strings.Contains(requestStr, "json_object") {
		fmt.Printf("[\n%v\n]\n", strings.TrimSuffix(strings.ReplaceAll(out.String(), "}\n", "},\n"), ",\n"))
	} else {
		fmt.Println(out.String())
	}
	if len(err.String()) > 2 {
		fmt.Println(err.String())
		os.Exit(9)
	}
}

func SqlTableStruct(tableName string) {
	var out bytes.Buffer
	cmd := exec.Command("sqlite3", "pacman.db", "pragma table_info('"+tableName+"');", "-cmd", ".header on", "-cmd", ".mode column")
	cmd.Stdout = &out
	cmd.Run()
	fmt.Println("::", COLOR_GREEN, tableName, COLOR_NONE)
	fmt.Println(out.String())
}
