package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	url_mirror = "https://mirror.netzspielplatz.de/manjaro/packages"
	// or https://manjaro.moson.eu/
	COLOR_NONE  = "\033[0m"
	COLOR_BLUE  = "\033[0;34m"
	COLOR_GREEN = "\033[0;36m"
	COLOR_RED   = "\033[38;5;124m"
	COLOR_GRAY  = "\033[38;5;243m"
	_VERSION    = "0.0.1"
	LocalDir    = "/.local/share/alpm-db/repos"
)

type tdesc map[string][]string

type Package struct {
	FILENAME    string
	dir         string
	id          int32
	NAME        string
	BASE        string `json:",omitempty"`
	VERSION     string
	DESC        string
	REPO        string
	URL         string   `json:",omitempty"`
	LICENSE     []string `json:",omitempty"`
	ARCH        string
	PACKAGER    string
	PROVIDES    []string `json:",omitempty"`
	CONFLICTS   []string `json:",omitempty"`
	DEPENDS     []string `json:",omitempty"`
	OPTDEPENDS  []string `json:",omitempty"`
	MAKEDEPENDS []string `json:",omitempty"`
	BUILDDATE   int64
	ISIZE       int
	CSIZE       int
}

func getFieldString(adesc tdesc, key string) string {
	if len(adesc[key]) < 1 {
		return ""
	}
	return strings.TrimSpace(adesc[key][0])
}

func getFieldArray(adesc tdesc, key string) []string {
	if len(adesc[key]) < 1 {
		return make([]string, 0)
	}
	//TOFIX last field in linux-lts removed ???
	for k, v := range adesc[key] { // remove descriptions
		adesc[key][k] = strings.TrimSpace(strings.SplitN(v, ":", 2)[0])
	}
	return adesc[key][0:]
}

func getFieldInt(adesc tdesc, key string) int {
	if items, ok := adesc[key]; ok {
		item := items[0]
		i, err := strconv.Atoi(item)
		if err == nil {
			return i
		}
	}
	return -1
}

// parse desc file content
func (p *Package) set(desc string) bool {
	//fields := []string{"FILENAME", "NAME", "VERSION", "URL", "DESC", "BASE"}

	tmpdesc := strings.Split(desc, "\n\n")
	/*if (p.dir == "stratis-cli-1.0.2-1/desc") {
		fmt.Println("stratis-cli desc origin:",desc)
	}*/
	adesc := make(tdesc)
	for i := range tmpdesc {
		tmp := strings.Split(tmpdesc[i], "\n")
		idx := strings.Replace(tmp[0], "%", "", -1)
		if len(tmp) > 1 {
			adesc[idx] = tmp[1:]
		} else {
			adesc[idx] = make([]string, 0)
		}
	}
	/*for k,v := range adesc {
		fmt.Println(k, "->", v)
	}*/

	p.VERSION = getFieldString(adesc, "VERSION")
	p.NAME = getFieldString(adesc, "NAME")
	/*if (p.NAME == "stratis-cli") {
		fmt.Println("stratis-cli adesc:",adesc)
		fmt.Println("stratis-cli:",p)
		//os.Exit(1)
	}*/
	p.DESC = getFieldString(adesc, "DESC")
	p.URL = getFieldString(adesc, "URL")
	p.BASE = getFieldString(adesc, "BASE")
	if p.BASE == p.NAME {
		p.BASE = ""
	}
	p.PACKAGER = getFieldString(adesc, "PACKAGER")
	p.ARCH = getFieldString(adesc, "ARCH")
	p.FILENAME = getFieldString(adesc, "FILENAME")

	p.LICENSE = getFieldArray(adesc, "LICENSE")
	p.DEPENDS = getFieldArray(adesc, "DEPENDS")
	p.MAKEDEPENDS = getFieldArray(adesc, "MAKEDEPENDS")
	p.OPTDEPENDS = getFieldArray(adesc, "OPTDEPENDS")
	p.PROVIDES = getFieldArray(adesc, "PROVIDES")
	p.CONFLICTS = getFieldArray(adesc, "CONFLICTS")

	p.BUILDDATE = int64(getFieldInt(adesc, "BUILDDATE"))
	p.CSIZE = getFieldInt(adesc, "CSIZE")
	p.ISIZE = getFieldInt(adesc, "ISIZE")

	//fmt.Println("\n--- package struct --- \n",p,"\n---")
	return true
}

func (p *Package) getBase() sql.NullString {
	if len(p.BASE) > 0 && p.BASE != p.NAME {
		return sql.NullString{
			String: p.BASE,
			Valid:  true,
		}
	}
	return sql.NullString{}
}

type Packages []Package

/*
 * find pakage by name
 * for replace sql too long replace package name field by field id
 * version only sql, gen : 37 seconds
 * version with FindByName() : 11 seconds !
 */
func (p *Packages) FindByName(name string) sql.NullInt32 {
	if strings.Contains(name, ".so") {
		return sql.NullInt32{}
	}
	for _, pkg := range *p {
		if pkg.NAME == name {
			return sql.NullInt32{
				Int32: int32(pkg.id),
				Valid: true,
			}
		}
	}
	return sql.NullInt32{}
}

type PackageFilter map[string]bool

/*
 * source: https://gist.github.com/indraniel/1a91458984179ab4cf80
 */
func ExtractTarGz(gzipStream io.Reader, pkgs Packages, repo string, filters PackageFilter) Packages {
	if len(filters) > 0 && len(pkgs) == len(filters) {
		return pkgs
	}
	uncompressedStream, err := gzip.NewReader(gzipStream)
	if err != nil {
		log.Fatal("ExtractTarGz: NewReader failed")
	}

	tarReader := tar.NewReader(uncompressedStream)

	for true {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			fmt.Println("err", err.Error())
			log.Fatalf("ExtractTarGz: Next() failed: %s", err.Error())
		}

		switch header.Typeflag {
		case tar.TypeDir:
			/*fmt.Println("::dir:",header.Name)*/
		case tar.TypeReg:

			buf := new(bytes.Buffer)

			if nb, err := buf.ReadFrom(tarReader); err != nil {
				if err != io.EOF {
					nb = nb + 1
					fmt.Println("error", err.Error())
					log.Fatalf("ExtractTarGz:  failed: %s", err.Error())
				}
			}

			pkg := Package{
				dir:  header.Name,
				REPO: repo,
				id:   int32(len(pkgs)) + 1,
			}
			if pkg.set(string(buf.Bytes())) {
				if len(filters) > 0 {
					if _, found := filters[pkg.NAME]; !found {
						continue
					}
					//pkgs = append(pkgs, pkg)
				}
				pkgs = append(pkgs, pkg)
				if len(filters) == 1 && len(pkgs) > 0 {
					return pkgs
				}
			}
		default:
			fmt.Println("error def", header.Typeflag, header.Name)
			log.Fatalf(
				"ExtractTarGz: uknown type: %s in %s",
				header.Typeflag,
				header.Name)
		}
	}
	return pkgs
}

func httpGetDb(url string, repo string, localDir string, ch chan<- string) {
	url = url + "/" + repo + "/x86_64/" + repo + ".db"
	if getParam("--arch") {
		url = url + "/" + repo + "/os/x86_64/" + repo + ".db"
	}
	localDir = localDir + "/" + repo + ".db"
	client := http.Client{Timeout: time.Duration(25) * time.Second}
	//fmt.Println(url + "...")
	resp, err := client.Get(url)
	if err != nil {
		ch <- err.Error()
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode > 399 {
		ch <- fmt.Sprintf("http error: %s: %v", url, resp.StatusCode)
		return
	}
	out, err := os.Create(localDir)
	if err != nil {
		ch <- fmt.Sprintf("os error: %s: %v", url, err)
		return
	}
	defer out.Close()
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		ch <- fmt.Sprintf("io error: %s: %v", url, err)
		return
	}
	ch <- url
}

func datasToJson(pkgs Packages) string {

	buff := &bytes.Buffer{}
	enc := json.NewEncoder(buff)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(&pkgs); err != nil {
		log.Println(err)
	}
	return strings.ReplaceAll(buff.String(), "},{", "},\n{")
}

func genJson(pkgs Packages) {

	fmt.Println("\n", COLOR_BLUE, "--- Json génération...", COLOR_NONE)
	// always in os.Getenv("HOME")+LocalDir ?

	os.Remove("./pacman.json")
	f, err := os.Create("./pacman.json")
	if err != nil {
		log.Fatal("Cannot create json file", err)
	}
	defer f.Close()

	tstart := time.Now() // start timer
	fmt.Fprintf(f, "%s", datasToJson(pkgs))
	telapsed := time.Since(tstart)
	fmt.Println("\njson duration: ", COLOR_GREEN, telapsed, COLOR_NONE, "\n ")
}

/*
 * parameter console is present ?
 */
func getParam(key string) bool {
	for _, arg := range os.Args[1:] {
		if arg == key {
			return true
		}
	}
	return false
}

/*
 * get console value after a parameter
 */
func getParamValue(key string, def string) string {
	for i, arg := range os.Args[1:] {
		if arg == key {
			if len(os.Args) > i+2 {
				return os.Args[i+2]
			} else {
				return def
			}
		}
	}
	return def
}

/*
 * get console values after a parameter
 */
func getParamValues(key string) (ret PackageFilter) {
	ret = make(PackageFilter, len(os.Args)-2)
	for i, arg := range os.Args[1:] {
		if arg == key {
			j := 2
			for len(os.Args) > i+j {
				nextv := os.Args[i+j]
				if strings.HasPrefix(nextv, "-") {
					return ret
				}
				ret[nextv] = true
				j = j + 1
			}
		}
	}
	return ret
}

func main() {
	os.MkdirAll(os.Getenv("HOME")+LocalDir, os.ModeDir|0777)

	a := getParamValue("-h", "x")
	fmt.Println(os.Args, "-h", "=>", a)
	b := getParam("--help")
	fmt.Println(os.Args, "--help", "=>", b)

	packagesFilter := getParamValues("-p")
	fmt.Println("packages:", packagesFilter)
	//os.Exit(0)

	if getParam("-h") || getParam("--help") {
		fmt.Println("")
		fmt.Println(COLOR_GRAY, "[ENV=\"\"]", COLOR_NONE, "./alpm-db [--json] [--sql] [-m mirrorurl] [--arch] [-b branch] [-q sql] [-p [values]]")
		fmt.Println("  --sql    : create sqlite3 ./pacman.db")
		fmt.Println("  --json   : create ./pacman.json")
		fmt.Println("  -b testing      : change branch (stable default)")
		fmt.Println("  -m \"https://xx\" : use different mirror (\"local\": use local pacman db)")
		fmt.Println("  --arch          : use archlinux mirror format")
		fmt.Println("  -p [packages]   : find and display json of this packages")
		fmt.Println("")
		fmt.Println("  -q \"SELECT * FROM pkgs\" : run sqlite command (pacman.db)")
		//TODO format output ??
		fmt.Println("")
		fmt.Println("Downloads in :", os.Getenv("HOME")+LocalDir)
		os.Exit(0)
	}

	arg := getParamValue("-q", "")
	if arg != "" {
		//RequestSql(os.Args[i+2])
		RunSql("", arg)
		os.Exit(0)
	}
	if getParam("-i") || getParam("--info") {
		SqlTableStruct("pkgs")
		SqlTableStruct("depends")
		SqlTableStruct("makedepends")
		SqlTableStruct("packagers")
		SqlTableStruct("repos")

		RunSql("Mainteners", "SELECT count(name) as 'packages', packagers.packager, packagers.id FROM pkgs LEFT JOIN packagers ON pkgs.packager=packagers.id GROUP BY packagers.id HAVING packagers.packager LIKE '%manjaro%' order by packages DESC")
		os.Exit(0)
	}

	LocalRepos := "/var/lib/pacman/sync"
	repos := []string{"core", "extra", "community", "multilib"}

	url := getParamValue("-m", url_mirror)
	branch := getParamValue("-b", "stable")

	if url != "local" {
		LocalRepos = os.Getenv("HOME") + LocalDir
		fmt.Println("\n", COLOR_BLUE, "--- Download repos...", COLOR_NONE)
		tstart := time.Now() // start timer
		ch := make(chan string)
		for _, repo := range repos {
			os.Remove(LocalRepos + "/" + repo + ".db")
			println(LocalRepos + "/" + repo + ".db")
			go httpGetDb(url+"/"+branch, repo, LocalRepos, ch)
		}
		for range repos {
			fmt.Println(<-ch)
		}
		telapsed := time.Since(tstart)
		fmt.Println("\nduration: ", COLOR_GREEN, telapsed, COLOR_NONE, "\n ")
	}

	fmt.Println("\n", COLOR_BLUE, "--- Parse files...", COLOR_NONE)
	var pkgs Packages
	tstart := time.Now() // start timer

	for _, repo := range repos {
		nb := len(pkgs)
		fmt.Println("::", repo, "...")
		f, err := os.Open(LocalRepos + "/" + repo + ".db")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		defer f.Close()
		pkgs = ExtractTarGz(f, pkgs, repo, packagesFilter)
		fmt.Println(repo, len(pkgs)-nb, "packages")
	}
	telapsed := time.Since(tstart)
	fmt.Println("\nduration: ", COLOR_GREEN, telapsed, COLOR_NONE, "\n ")
	fmt.Println("\n=>", len(pkgs), "packages")

	if getParam("--json") {
		genJson(pkgs)
	}
	if getParam("--sql") {
		GenSqlite(pkgs)
	}

	if len(packagesFilter) > 0 {
		fmt.Println(datasToJson(pkgs))
	}

}

/*


MIRROR="https://manjaro.moson.eu" ./ls-alpm		# change mirror
MIRROR=0 ./ls-alpm								# use /var/lib/pacman/


 ./ls-alpm -q "select json_object('name',name,'version',version, 'build', builddate) FROM pkgs WHERE name LIKE 'pac%'"
./ls-alpm -q "select json_object('name',name,'version',version) FROM pkgs LEFT JOIN depends ON pkgs.id=depends.id WHERE depends.depend='gtk2'"



SELECT '['||group_concat(depend)||']' as deps_list FROM depends WHERE id=13
-> [glibc,m4,sh]
# pacman with dependencies :
SELECT name, '['||group_concat(depend)||']' as deps_list FROM pkgs INNER JOIN depends ON pkgs.id=depends.id WHERE name LIKE 'pacman'



SELECT name, depend FROM pkgs LEFT JOIN depends ON pkgs.id=depends.id WHERE depend LIKE '%=%'
SELECT name, depend FROM pkgs LEFT JOIN depends ON pkgs.id=depends.id WHERE depend LIKE '.so'

SELECT pkgs.id, name, depend , comp, ver FROM pkgs LEFT JOIN depends ON pkgs.id=depends.id WHERE depend='python' ORDER by ver ASC

*/
