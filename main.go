package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "gopkg.in/cheggaaa/pb.v1"
)

type payload struct {
	counter int
	record  []string
}

type initConfig struct {
	CentZip         int
	MaxRadius       int
	MaxVehYear      int
	MinVehYear      int
	MaxYearDelDate  int
	MinYearDelDate  int
	Vendor          string
	Source          string
	DelBlankDATE    bool
	DelBlankDELDATE bool
	Headers         []string
}

type resources struct {
	param  initConfig
	cord   map[string][]string
	scfFac map[string]string
	dduFac map[string]string
	hist   map[string]int
	dnm    map[string]int
	genS   map[string]int
	genSNm map[string]int
}

func main() {
	munger()
}

func munger() {
	start := time.Now()
	gophers := flag.Int("C", 10, "Set workers to run in parallel")
	flag.Parse()

	for _, v := range readDir() {
		var (
			counter int
			outfile = fmt.Sprintf("%v_output.csv", v[:len(v)-4])
			colMap  map[int]int
		)
		bar := pb.StartNew(rowCount(v))
		file, err := os.Open(v)
		if err != nil {
			log.Fatalln("Error opening source file", err)
		}
		defer file.Close()

		resource := resources{
			param:  loadConfig(),
			cord:   loadZipCor(),
			scfFac: loadSCFFac(),
			dduFac: loadDDUFac(),
			hist:   loadHist(),
			dnm:    loadDNM(),
			genS:   loadGenS(),
			genSNm: loadGenSNm(),
		}
		hcm := constHeaderMap(resource.param.Headers)

		tasks := make(chan payload)
		go func() {
			rows, err := csv.NewReader(file).ReadAll()
			if err != nil {
				log.Fatalln("Error reading source row", err)
			}
			for i, row := range rows {
				counter = i
				if i == 0 {
					colMap = setCol(payload{
						counter: i,
						record:  row,
					}, hcm)
				} else {
					tasks <- mapCol(payload{
						counter: i,
						record:  row,
					}, colMap, resource)
				}
			}
			close(tasks)
		}()

		results := make(chan payload)
		var wg sync.WaitGroup
		wg.Add(*gophers)

		go func() {
			wg.Wait()
			close(results)
		}()

		for i := 0; i < *gophers; i++ {
			go func() {
				defer wg.Done()
				for t := range tasks {
					results <- process(t, resource, hcm)
					bar.Increment()
				}
			}()
		}
		outputCSV(outfile, resource, results, hcm)
		fmt.Printf("Elapsed Time: %v, Total: %v\n", time.Since(start), counter)
	}
}

func tCase(f string) string {
	return strings.TrimSpace(strings.Title(strings.ToLower(f)))
}
func uCase(f string) string {
	return strings.TrimSpace(strings.ToUpper(f))
}
func lCase(f string) string {
	return strings.TrimSpace(strings.ToLower(f))
}
func cInt(f string) int {
	i, err := strconv.Atoi(f)
	if err != nil {
		log.Fatalf("Error converting string to int %v (%v)", err, f)
	}
	return i
}
func stdAddress(f string) string {
	return strings.Title(strings.ToLower(strings.Join(strings.Fields(f), " ")))
}

func decYr(y string) string {
	// YearDecodeDict is a map of 2-Digit abbreviated Years
	yrDecDict := map[string]string{"0": "2000", "1": "2001", "2": "2002",
		"3": "2003", "4": "2004", "5": "2005", "6": "2006", "7": "2007",
		"8": "2008", "9": "2009", "10": "2010", "11": "2011", "12": "2012",
		"13": "2013", "14": "2014", "15": "2015", "16": "2016", "17": "2017",
		"18": "2018", "19": "2019", "20": "2020", "40": "1940", "41": "1941",
		"42": "1942", "43": "1943", "44": "1944", "45": "1945", "46": "1946",
		"47": "1947", "48": "1948", "49": "1949", "50": "1950", "51": "1951",
		"52": "1952", "53": "1953", "54": "1954", "55": "1955", "56": "1956",
		"57": "1957", "58": "1958", "59": "1959", "60": "1960", "61": "1961",
		"62": "1962", "63": "1963", "64": "1964", "65": "1965", "66": "1966",
		"67": "1967", "68": "1968", "69": "1969", "70": "1970", "71": "1971",
		"72": "1972", "73": "1973", "74": "1974", "75": "1975", "76": "1976",
		"77": "1977", "78": "1978", "79": "1979", "80": "1980", "81": "1981",
		"82": "1982", "83": "1983", "84": "1984", "85": "1985", "86": "1986",
		"87": "1987", "88": "1988", "89": "1989", "90": "1990", "91": "1991",
		"92": "1992", "93": "1993", "94": "1994", "95": "1995", "96": "1996",
		"97": "1997", "98": "1998", "99": "1999"}
	if dy, ok := yrDecDict[y]; ok {
		return dy
	}
	return y
}
func decAbSt(s string) string {
	// UsStatesDict is a map of 2-Digit abbreviated US States
	usStDict := map[string]string{"AK": "Alaska", "AL": "Alabama",
		"AR": "Arkansas", "AS": "American Samoa", "AZ": "Arizona",
		"CA": "California", "CO": "Colorado", "CT": "Connecticut",
		"DC": "District of Columbia", "DE": "Delaware", "FL": "Florida",
		"GA": "Georgia", "GU": "Guam", "HI": "Hawaii", "IA": "Iowa",
		"ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "KS": "Kansas",
		"KY": "Kentucky", "LA": "Louisiana", "MA": "Massachusetts",
		"MD": "Maryland", "ME": "Maine", "MI": "Michigan", "MN": "Minnesota",
		"MO": "Missouri", "MP": "Northern Mariana Islands", "MS": "Mississippi",
		"MT": "Montana", "NA": "National", "NC": "North Carolina",
		"ND": "North Dakota", "NE": "Nebraska", "NH": "New Hampshire",
		"NJ": "New Jersey", "NM": "New Mexico", "NV": "Nevada", "NY": "New York",
		"OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
		"PR": "Puerto Rico", "RI": "Rhode Island", "SC": "South Carolina",
		"SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
		"VA": "Virginia", "VI": "Virgin Islands", "VT": "Vermont",
		"WA": "Washington", "WI": "Wisconsin", "WV": "West Virginia",
		"WY": "Wyoming"}
	if ds, ok := usStDict[s]; ok {
		return ds
	}
	return s
}

func setSCF(s string) string {
	switch len(s) {
	case 5:
		if s[:1] != "0" {
			return s[:3]
		}
		return s[1:3]
	case 4:
		return s[:2]
	}
	return ""
}

func valZip(p string) string {
	switch {
	case regexp.MustCompile(`^[0-9][0-9][0-9][0-9]$`).MatchString(p):
		return p
	case regexp.MustCompile(`^[0-9][0-9][0-9][0-9][0-9]$`).MatchString(p):
		if p[:1] == "0" {
			p = p[1:]
			return p
		}
		return p
	case regexp.MustCompile(`^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$`).MatchString(p):
		return p[:5]
	case regexp.MustCompile(`^[0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]$`).MatchString(p):
		x := strings.Split(p, "-")
		return x[0]
	}
	return ""
}

func reformatPhone(p string) string {
	sep := []string{"-", ".", "*", "(", ")"}
	for _, v := range sep {
		p = strings.Replace(p, v, "", -1)
	}
	p = strings.Replace(p, " ", "", -1)
	switch len(p) {
	case 10:
		p = fmt.Sprintf("(%v) %v-%v", p[0:3], p[3:6], p[6:10])
	case 7:
		p = fmt.Sprintf("%v-%v", p[0:3], p[3:7])
	default:
		p = ""
	}
	return p
}

func loadConfig() initConfig {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalln("Cannot get pwd", err)
	}
	rescPath := fmt.Sprintf("/Users/%v/Dropbox/Resource/", strings.Split(pwd, "/")[2])

	conf, err := os.Open(filepath.Join(rescPath, "config.json"))
	if err != nil {
		log.Fatalln(err)
	}
	defer conf.Close()

	var param initConfig

	jsonParser := json.NewDecoder(conf)
	if err = jsonParser.Decode(&param); err != nil {
		log.Fatalln("error decoding json file", err)
	}
	return param
}

func loadZipCor() map[string][]string {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalln("Cannot get pwd", err)
	}
	rescPath := fmt.Sprintf("/Users/%v/Dropbox/Resource/", strings.Split(pwd, "/")[2])

	cord := make(map[string][]string)

	zipCor, err := os.Open(filepath.Join(rescPath, "USZIPCoordinates.csv"))
	if err != nil {
		log.Fatalln("Cannot open ZipCoord file", err)
	}
	defer zipCor.Close()
	rdr := csv.NewReader(zipCor)
	for {
		z, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		cord[z[0]] = []string{z[1], z[2]}
	}
	return cord
}

func loadSCFFac() map[string]string {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalln("Cannot get pwd", err)
	}
	rescPath := fmt.Sprintf("/Users/%v/Dropbox/Resource/", strings.Split(pwd, "/")[2])

	scf := make(map[string]string)

	f, err := os.Open(filepath.Join(rescPath, "SCFFacilites.csv"))
	if err != nil {
		log.Fatalln("Cannot open SCFFac file", err)
	}
	defer f.Close()
	rdr := csv.NewReader(f)
	for {
		s, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		scf[s[0]] = s[1]
	}
	return scf
}

func loadDDUFac() map[string]string {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalln("Cannot get pwd", err)
	}
	rescPath := fmt.Sprintf("/Users/%v/Dropbox/Resource/", strings.Split(pwd, "/")[2])

	ddu := make(map[string]string)

	f, err := os.Open(filepath.Join(rescPath, "DDUFacilites.csv"))
	if err != nil {
		log.Fatalln("Cannot open DDUFac file", err)
	}
	defer f.Close()
	rdr := csv.NewReader(f)
	for {
		s, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		ddu[s[0]] = s[1]
	}
	return ddu
}

func loadHist() map[string]int {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalln("Cannot get pwd", err)
	}
	rescPath := fmt.Sprintf("/Users/%v/Dropbox/Resource/", strings.Split(pwd, "/")[2])

	hisp := make(map[string]int)

	f, err := os.Open(filepath.Join(rescPath, "HispLNames.csv"))
	if err != nil {
		log.Fatalln("Cannot open Hisp file", err)
	}
	defer f.Close()
	rdr := csv.NewReader(f)
	for {
		s, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		hisp[tCase(s[0])]++
	}
	return hisp
}

func loadDNM() map[string]int {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalln("Cannot get pwd", err)
	}
	rescPath := fmt.Sprintf("/Users/%v/Dropbox/Resource/", strings.Split(pwd, "/")[2])

	dnm := make(map[string]int)

	f, err := os.Open(filepath.Join(rescPath, "DoNotMail.csv"))
	if err != nil {
		log.Fatalln("Cannot open DNM file", err)
	}
	defer f.Close()
	rdr := csv.NewReader(f)
	for {
		s, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		dnm[tCase(s[0])]++
	}
	return dnm
}

func loadGenS() map[string]int {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalln("Cannot get pwd", err)
	}
	rescPath := fmt.Sprintf("/Users/%v/Dropbox/Resource/", strings.Split(pwd, "/")[2])

	gen := make(map[string]int)

	f, err := os.Open(filepath.Join(rescPath, "_GeneralSuppression.csv"))
	if err != nil {
		log.Fatalln("Cannot open GenSup file", err)
	}
	defer f.Close()
	rdr := csv.NewReader(f)
	for {
		s, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		adrZip := fmt.Sprintf("%v %v", tCase(s[2]), tCase(s[5]))
		gen[adrZip]++
	}
	return gen
}

func loadGenSNm() map[string]int {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalln("Cannot get pwd", err)
	}
	rescPath := fmt.Sprintf("/Users/%v/Dropbox/Resource/", strings.Split(pwd, "/")[2])

	gen := make(map[string]int)

	f, err := os.Open(filepath.Join(rescPath, "_GeneralSuppressionNames.csv"))
	if err != nil {
		log.Fatalln("Cannot open GenSup file", err)
	}
	defer f.Close()
	rdr := csv.NewReader(f)
	for {
		s, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		fnln := fmt.Sprintf("%v %v", tCase(s[0]), tCase(s[1]))
		gen[fnln]++
	}
	return gen
}

func hsin(theta float64) float64 {
	// haversin(θ) function
	return math.Pow(math.Sin(theta/2), 2)
}

func distance(lat1, lon1, lat2, lon2 float64) float64 {
	// convert to radians, must cast radius as float to multiply later
	var la1, lo1, la2, lo2, rad float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180
	rad = 3959 // Earth radius in Miles
	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)
	return 2 * rad * math.Asin(math.Sqrt(h))
}

func getLatLong(cZip, rZip string, res resources) (float64, float64, float64, float64) {
	// Validate Record ZIP
	recCor, OKrZip := res.cord[rZip]
	if !OKrZip {
		log.Printf("Invalid Record Zip Code : %v", rZip)
	}
	// Validate Central ZIP
	cenCor, OKcZip := res.cord[cZip]
	if !OKcZip {
		log.Printf("Invalid Central Zip Code : %v", cZip)
	}
	// convert Coordinates tin FLoat64
	lat1, err := strconv.ParseFloat(cenCor[0], 64)
	lon1, err := strconv.ParseFloat(cenCor[1], 64)
	lat2, err := strconv.ParseFloat(recCor[0], 64)
	lon2, err := strconv.ParseFloat(recCor[1], 64)
	if err != nil {
		log.Fatalln("Error converting coordinates", err)
	}
	return lat1, lon1, lat2, lon2
}

func parseDate(d string) (string, string, string, string) {
	if d != "" {
		formats := []string{"1/2/2006", "1-2-2006", "1/2/06", "1-2-06",
			"2006/1/2", "2006-1-2"}
		for _, f := range formats {
			if t, err := time.Parse(f, d); err == nil {
				nDate := fmt.Sprintf("%v/%v/%v", strconv.Itoa(t.Year()), strconv.Itoa(int(t.Month())), strconv.Itoa(t.Day()))
				return nDate, strconv.Itoa(t.Year()), strconv.Itoa(int(t.Month())), strconv.Itoa(t.Day())
			}
		}
	}
	return "", "", "", ""
}

func checkSalut(f string) bool {
	salutations := []string{"MR", "MR.", "MS", "MS.", "MRS", "MRS.", "DR",
		"DR.", "MISS", "CORP", "SGT", "PVT", "CAPT", "COL", "MAJ", "LT",
		"LIEUTENANT", "PRM", "PATROLMAN", "HON", "OFFICER", "REV", "PRES",
		"PRESIDENT", "GOV", "GOVERNOR", "VICE PRESIDENT", "VP", "MAYOR",
		"SIR", "MADAM", "HONORABLE"}
	for _, salu := range salutations {
		if tCase(f) == tCase(salu) {
			return true
		}
	}
	return false
}

func checkSep(f string) bool {
	separators := []string{"&", "AND", "OR", "/"}
	for _, sep := range separators {
		if tCase(f) == tCase(sep) {
			return true
		}
	}
	return false
}

func checkSuf(f string) bool {
	suffixes := []string{"ESQ", "PHD", "MD", "TRUE"}
	for _, suf := range suffixes {
		if tCase(f) == tCase(suf) {
			return true
		}
	}
	return false
}

func checklnPref(f string) bool {
	lnPrefixes := []string{"DE", "DA", "DI", "LA", "LOS", "DU", "DEL",
		"DEI", "VDA", "DELLO", "DELLA", "DEGLI", "DELLE", "VAN", "VON",
		"DER", "DEN", "MC", "HEER", "TEN", "TER", "VANDE", "VANDEN",
		"VANDER", "VOOR", "VER", "AAN", "MC", "SAN", "SAINZ", "BIN", "LI",
		"LE", "DES", "AM", "AUS'M", "VOM", "ZUM", "ZUR", "TEN", "IBN",
		"ABUa", "BON", "BIN", "DAL", "DER", "IBN", "LE", "ST", "STE", "VAN",
		"VEL", "VON"}
	for _, pref := range lnPrefixes {
		if tCase(f) == tCase(pref) {
			return true
		}
	}
	return false
}

func checkGener(f string) bool {
	generations := []string{"JR", "SR", "I", "II", "III", "IV", "V", "VI",
		"VII", "VIII", "IX", "X", "1ST", "2ND", "3RD", "4TH", "5TH", "6TH",
		"7TH", "8TH", "9TH", "10TH", "FIRST", "SECOND", "THIRD", "FOURTH",
		"FIFTH", "SIXTH", "SEVENTH", "EIGHTH", "NINTH", "TENTH"}
	for _, gen := range generations {
		if tCase(f) == tCase(gen) {
			return true
		}
	}
	return false
}

func checkNameSeparators(sl []string) []string {
	var nsl []string
	var gen string
	for i := 1; i <= len(sl); i++ {
		v := tCase(sl[len(sl)-i])
		if checkSep(v) {
			if i < 3 {
				continue
			} else {
				break
			}
		} else if checkSuf(v) {
			continue
		} else if checkSalut(v) {
			continue
		} else if checkGener(v) {
			if i == 1 {
				gen = v
			}
			continue
		} else if checklnPref(v) {
			for _, s := range nsl {
				v = fmt.Sprintf("%v %v", v, s)
			}
			nsl = []string{}
		}
		nsl = append([]string{v}, nsl...)
	}
	if gen != "" {
		nsl[len(nsl)-1] = fmt.Sprintf("%v %v", nsl[len(nsl)-1], gen)
		return nsl
	}
	return nsl
}

func parseFullName(fn string) (string, string, string) {
	fnSplit := checkNameSeparators(strings.Fields(fn))
	switch len(fnSplit) {
	case 1:
		return fnSplit[0], "", ""
	case 2:
		return fnSplit[0], "", fnSplit[1]
	case 3:
		if len(fnSplit[2]) == 1 && len(fnSplit[1]) > 2 {
			return fnSplit[0], "", fnSplit[1]
		}
		return fnSplit[0], fnSplit[1], fnSplit[2]
	case 4:
		return fnSplit[0], "", fnSplit[3]
	case 5:
		return fnSplit[0], "", fnSplit[4]
	}
	return "", "", ""
}

func setCol(r payload, hdr map[string]int) map[int]int {
	hasZip := false
	c := make(map[int]int)
	for i, v := range r.record {
		switch {
		case regexp.MustCompile(`(?i)cust.+id`).MatchString(v):
			if _, ok := hdr["customerid"]; ok {
				c[hdr["customerid"]] = i
			}
		case regexp.MustCompile(`(?i)ful.+me`).MatchString(v):
			if _, ok := hdr["fullname"]; ok {
				c[hdr["fullname"]] = i
			}
		case regexp.MustCompile(`(?i)fir.+me`).MatchString(v):
			if _, ok := hdr["firstname"]; ok {
				c[hdr["firstname"]] = i
			}
		case regexp.MustCompile(`(?i)^mi$`).MatchString(v):
			if _, ok := hdr["mi"]; ok {
				c[hdr["mi"]] = i
			}
		case regexp.MustCompile(`(?i)las.+me`).MatchString(v):
			if _, ok := hdr["lastname"]; ok {
				c[hdr["lastname"]] = i
			}
		case regexp.MustCompile(`(?i)^address$`).MatchString(v):
			if _, ok := hdr["address1"]; ok {
				c[hdr["address1"]] = i
			}
		case regexp.MustCompile(`(?i)addr.+1`).MatchString(v):
			if _, ok := hdr["address1"]; ok {
				c[hdr["address1"]] = i
			}
		case regexp.MustCompile(`(?i)addr.+2`).MatchString(v):
			if _, ok := hdr["address2"]; ok {
				c[hdr["address2"]] = i
			}
		case regexp.MustCompile(`(?i)^city$`).MatchString(v):
			if _, ok := hdr["city"]; ok {
				c[hdr["city"]] = i
			}
		case regexp.MustCompile(`(?i)^state$`).MatchString(v):
			if _, ok := hdr["state"]; ok {
				c[hdr["state"]] = i
			}
		case regexp.MustCompile(`(?i)^zip$`).MatchString(v):
			if _, ok := hdr["zip"]; ok {
				c[hdr["zip"]] = i
			}
			hasZip = true
		case regexp.MustCompile(`(?i)^4zip$`).MatchString(v):
			if _, ok := hdr["zip4"]; ok {
				c[hdr["zip4"]] = i
			}
		case regexp.MustCompile(`(?i)^zip4$`).MatchString(v):
			if _, ok := hdr["zip4"]; ok {
				c[hdr["zip4"]] = i
			}
		case regexp.MustCompile(`(?i)^hph$`).MatchString(v):
			if _, ok := hdr["hph"]; ok {
				c[hdr["hph"]] = i
			}
		case regexp.MustCompile(`(?i)^bph$`).MatchString(v):
			if _, ok := hdr["bph"]; ok {
				c[hdr["bph"]] = i
			}
		case regexp.MustCompile(`(?i)^cph$`).MatchString(v):
			if _, ok := hdr["cph"]; ok {
				c[hdr["cph"]] = i
			}
		case regexp.MustCompile(`(?i)^email$`).MatchString(v):
			if _, ok := hdr["email"]; ok {
				c[hdr["email"]] = i
			}
		case regexp.MustCompile(`(?i)^vin$`).MatchString(v):
			if _, ok := hdr["vin"]; ok {
				c[hdr["vin"]] = i
			}
		case regexp.MustCompile(`(?i)^year$`).MatchString(v):
			if _, ok := hdr["year"]; ok {
				c[hdr["year"]] = i
			}
		case regexp.MustCompile(`(?i)^vyr$`).MatchString(v):
			if _, ok := hdr["year"]; ok {
				c[hdr["year"]] = i
			}
		case regexp.MustCompile(`(?i)^make$`).MatchString(v):
			if _, ok := hdr["make"]; ok {
				c[hdr["make"]] = i
			}
		case regexp.MustCompile(`(?i)^vmk$`).MatchString(v):
			if _, ok := hdr["make"]; ok {
				c[hdr["make"]] = i
			}
		case regexp.MustCompile(`(?i)^model$`).MatchString(v):
			if _, ok := hdr["model"]; ok {
				c[hdr["model"]] = i
			}
		case regexp.MustCompile(`(?i)^vmd$`).MatchString(v):
			if _, ok := hdr["model"]; ok {
				c[hdr["model"]] = i
			}
		case regexp.MustCompile(`(?i)^DelDate$`).MatchString(v):
			if _, ok := hdr["deldate"]; ok {
				c[hdr["deldate"]] = i
			}
		case regexp.MustCompile(`(?i)^Date$`).MatchString(v):
			if _, ok := hdr["date"]; ok {
				c[hdr["date"]] = i
			}
		case regexp.MustCompile(`(?i)^DSF_WALK_SEQ$`).MatchString(v):
			if _, ok := hdr["dsfwalkseq"]; ok {
				c[hdr["dsfwalkseq"]] = i
			}
		case regexp.MustCompile(`(?i)^Crrt$`).MatchString(v):
			if _, ok := hdr["crrt"]; ok {
				c[hdr["crrt"]] = i
			}
		case regexp.MustCompile(`(?i)^KBB$`).MatchString(v):
			if _, ok := hdr["kbb"]; ok {
				c[hdr["kbb"]] = i
			}
		}
	}
	if hasZip == false {
		log.Fatalln("ERROR: ZIP code is a requried field")
	}
	return c
}

func mapCol(r payload, m map[int]int, res resources) payload {
	nr := make([]string, len(res.param.Headers))
	for i := range nr {
		_, ok := m[i]
		if ok {
			nr[i] = r.record[m[i]]
		}
	}
	r.record = nr
	return r
}

func process(pay payload, res resources, hdr map[string]int) payload {
	for i, v := range pay.record {
		switch i {
		case hdr["state"], hdr["vin"]:
			pay.record[i] = uCase(v)
		case hdr["email"]:
			pay.record[i] = lCase(v)
		case hdr["hph"], hdr["bph"], hdr["cph"]:
			pay.record[i] = reformatPhone(v)
		case hdr["address1"], hdr["address2"]:
			pay.record[i] = stdAddress(v)
		default:
			pay.record[i] = tCase(v)
		}
	}
	// Set customerid
	switch uCase(res.param.Source) {
	case "D":
		pay.record[hdr["customerid"]] = fmt.Sprintf("D%d", pay.counter+100000)
	case "P":
		pay.record[hdr["customerid"]] = fmt.Sprintf("P%d", pay.counter+500000)
	default:
		pay.record[hdr["customerid"]] = fmt.Sprintf("%06d", pay.counter)
	}

	// Parse FullName if FirstName & LastName == ""
	if pay.record[hdr["fullname"]] != "" && pay.record[hdr["firstname"]] == "" && pay.record[hdr["lastname"]] == "" {
		pay.record[hdr["firstname"]], pay.record[hdr["mi"]], pay.record[hdr["lastname"]] = parseFullName(pay.record[hdr["fullname"]])
	}

	// Combine FirstName + LastName to FullName
	if pay.record[hdr["fullname"]] == "" {
		pay.record[hdr["fullname"]] = fmt.Sprintf("%v %v", pay.record[hdr["firstname"]], pay.record[hdr["lastname"]])
	}

	// Combine address1 + Address2 to AddressFull
	pay.record[hdr["addressfull"]] = fmt.Sprintf("%v %v", pay.record[hdr["address1"]], pay.record[hdr["address2"]])

	// Set Phone field based on availability of hph, bph & cph
	switch {
	case pay.record[hdr["hph"]] != "":
		pay.record[hdr["phone"]] = pay.record[hdr["hph"]]
	case pay.record[hdr["bph"]] != "":
		pay.record[hdr["phone"]] = pay.record[hdr["bph"]]
	case pay.record[hdr["cph"]] != "":
		pay.record[hdr["phone"]] = pay.record[hdr["cph"]]
	}
	// Set VINlen
	pay.record[hdr["vinlen"]] = fmt.Sprint(len(pay.record[hdr["vin"]]))
	// Set ZipCrrt
	pay.record[hdr["zipcrrt"]] = fmt.Sprintf("%v%v", pay.record[hdr["zip"]], pay.record[hdr["crrt"]])

	// Validate Central Zipcode
	_, okCzip := res.cord[valZip(strconv.Itoa(res.param.CentZip))]
	if !okCzip {
		log.Fatalln("Invalid Central Zip Code")
	}
	// Standardize Zipcode
	pay.record[hdr["zip"]] = valZip(pay.record[hdr["zip"]])
	// Validate record Zipcode
	_, okRzip := res.cord[pay.record[hdr["zip"]]]
	if !okRzip {
		log.Printf("Invalid Zip Code on row %v, zip code %v (%v, %v) ", pay.counter, pay.record[hdr["zip"]], pay.record[hdr["city"]], pay.record[hdr["state"]])
	}
	if okRzip && okCzip {
		// Set Radius(miles) based on Central Zip and Row Zip
		clat1, clon2, rlat1, rlon2 := getLatLong(strconv.Itoa(res.param.CentZip), pay.record[hdr["zip"]], res)
		pay.record[hdr["radius"]] = fmt.Sprintf("%.2f", distance(clat1, clon2, rlat1, rlon2))
		// Set Coordinte value
		pay.record[hdr["coordinates"]] = fmt.Sprintf("%v,%v", rlat1, rlon2)
	}

	// Set DelDate, Date, Dld_Year, Dld_Month, Dld_Day
	pay.record[hdr["deldate"]], pay.record[hdr["dldyear"]], pay.record[hdr["dldmonth"]], pay.record[hdr["dldday"]] = parseDate(pay.record[hdr["deldate"]])
	pay.record[hdr["date"]], pay.record[hdr["lsdyear"]], pay.record[hdr["lsdmonth"]], pay.record[hdr["lsdday"]] = parseDate(pay.record[hdr["date"]])

	// Set Extended State Value
	pay.record[hdr["expandedstate"]] = decAbSt(pay.record[hdr["state"]])

	// Set SCF value
	pay.record[hdr["scf"]] = setSCF(pay.record[hdr["zip"]])

	// Set DDU Faculity
	if ddufac, ok := res.dduFac[pay.record[hdr["zip"]]]; ok {
		pay.record[hdr["ddufacility"]] = ddufac
	}

	// Set SCF Faculity
	if scffac, ok := res.scfFac[pay.record[hdr["scf"]]]; ok {
		pay.record[hdr["scf3dfacility"]] = scffac
	}

	// Set Ethnicity
	if _, ok := res.hist[pay.record[hdr["lastname"]]]; ok {
		pay.record[hdr["ethnicity"]] = "Hisp"
	}

	// Set Vendor
	pay.record[hdr["vendor"]] = res.param.Vendor

	return pay
}

func outputCSV(out string, res resources, results <-chan payload, hcm map[string]int) {
	f, err := os.Create(out)
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write(res.param.Headers)

	for r := range results {
		if err := w.Write(r.record); err != nil {
			log.Fatalln(err)
		}
	}
	w.Flush()
}

func rowCount(fn string) int {
	out, err := exec.Command("wc", "-l", fn).Output()
	if err != nil {
		log.Fatalln("unable to run UNIX cmd", err)
	}
	int, err := strconv.Atoi(strings.Fields(string(out))[0])
	if err != nil {
		log.Fatalln("unable to conv []bytes", err)
	}
	return int
}

func readDir() []string {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatalln("Error reading Directory", err)
	}
	var f []string
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".csv" {
			f = append(f, file.Name())
		}
	}
	if len(f) < 1 {
		log.Fatalln("Directory does not contain a .csv file")
	}
	return f
}

func constHeaderMap(h []string) map[string]int {
	defheaders := map[string]int{"customerid": 0, "fullname": 1,
		"firstname": 2, "mi": 3, "lastname": 4, "address1": 5, "address2": 6,
		"addressfull": 7, "city": 8, "state": 9, "zip": 10, "zip4": 11,
		"scf": 12, "phone": 13, "hph": 14, "bph": 15, "cph": 16, "email": 17,
		"vin": 18, "year": 19, "make": 20, "model": 21, "deldate": 22,
		"date": 23, "radius": 24, "coordinates": 25, "vinlen": 26,
		"dsfwalkseq": 27, "crrt": 28, "zipcrrt": 29, "KBB": 30,
		"buybackvalue": 31, "winnum": 32, "maildnq": 33, "blitzdnq": 34,
		"drop": 35, "purl": 36, "ddufacility": 37, "scf3dfacility": 38,
		"vendor": 39, "expandedstate": 40, "ethnicity": 41, "dldyear": 42,
		"dldmonth": 43, "dldday": 44, "lsdyear": 45, "lsdmonth": 46,
		"lsdday": 47, "misc1": 48, "misc2": 49, "misc3": 50}
	if len(h) == 51 {
		for _, v := range h {
			if _, ok := defheaders[lCase(v)]; !ok {
				log.Println("[ Incompatible headers, using default headers ]")
				return defheaders
			}
			header := make(map[string]int)
			for i, v := range h {
				header[lCase(v)] = i
			}
			return header
		}
	}
	log.Println("[ Missing required headers, using default headers ]")
	return defheaders
}
