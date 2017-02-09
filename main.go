package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	custID        = iota // 0
	fullName             // 1
	firstName            // 2
	mi                   // 3
	lastName             // 4
	address1             // 5
	address2             // 6
	addressFull          // 7
	city                 // 8
	state                // 9
	zip                  // 10
	zip4                 // 11
	scf                  // 12
	phone                // 13
	hph                  // 14
	bph                  // 15
	cph                  // 16
	email                // 17
	vin                  // 18
	vyear                // 19
	vmake                // 20
	vmodel               // 21
	delDate              // 22
	date                 // 23
	radius               // 24
	coordinates          // 25
	vinLen               // 26
	dsfwalkseq           // 27
	crrt                 // 28
	zipCrrt              // 29
	kbb                  // 30
	buybackValue         // 31
	winNum               // 32
	mailDnq              // 33
	blitzDnq             // 34
	drop                 // 35
	purl                 // 36
	dduFacility          // 37
	scf3dFacility        // 38
	vendor               // 39
	expandedState        // 40
	ethnicity            // 41
	dldYear              // 42
	dldMonth             // 43
	dldDay               // 44
	misc1                // 45
	misc2                // 46
	misc3                // 47
)

var header = []string{
	"CustomerID",
	"FullName",
	"FirstName",
	"MI",
	"LastName",
	"Address1",
	"Address2",
	"AddressFull",
	"City",
	"State",
	"Zip",
	"4Zip",
	"SCF",
	"Phone",
	"HPH",
	"BPH",
	"CPH",
	"Email",
	"VIN",
	"Year",
	"Make",
	"Model",
	"DelDate",
	"Date",
	"Radius",
	"Coordinates",
	"VINLen",
	"DSF_WALK_SEQ",
	"Crrt",
	"ZipCrrt",
	"KBB",
	"Buyback_Value",
	"WinningNumber",
	"MailDNQ",
	"BlitzDNQ",
	"Drop",
	"PURL",
	"DDUFacility",
	"SCF3DFacility",
	"Vendor",
	"ExpandedState",
	"Ethnicity",
	"Dld_Year",
	"Dld_Month",
	"Dld_Day",
	"Misc1",
	"Misc2",
	"Misc",
}

// Global Variables
// YearDecodeDict is a map of 2-Digit abbreviated Years
var yrDecDict = map[string]string{"0": "2000",
	"1": "2001", "2": "2002", "3": "2003", "4": "2004", "5": "2005", "6": "2006", "7": "2007",
	"8": "2008", "9": "2009", "10": "2010", "11": "2011", "12": "2012", "13": "2013", "14": "2014",
	"15": "2015", "16": "2016", "17": "2017", "18": "2018", "19": "2019", "20": "2020", "40": "1940",
	"41": "1941", "42": "1942", "43": "1943", "44": "1944", "45": "1945", "46": "1946", "47": "1947",
	"48": "1948", "49": "1949", "50": "1950", "51": "1951", "52": "1952", "53": "1953", "54": "1954",
	"55": "1955", "56": "1956", "57": "1957", "58": "1958", "59": "1959", "60": "1960", "61": "1961",
	"62": "1962", "63": "1963", "64": "1964", "65": "1965", "66": "1966", "67": "1967", "68": "1968",
	"69": "1969", "70": "1970", "71": "1971", "72": "1972", "73": "1973", "74": "1974", "75": "1975",
	"76": "1976", "77": "1977", "78": "1978", "79": "1979", "80": "1980", "81": "1981", "82": "1982",
	"83": "1983", "84": "1984", "85": "1985", "86": "1986", "87": "1987", "88": "1988", "89": "1989",
	"90": "1990", "91": "1991", "92": "1992", "93": "1993", "94": "1994", "95": "1995", "96": "1996",
	"97": "1997", "98": "1998", "99": "1999"}

// UsStatesDict is a map of 2-Digit abbreviated US States
var usStDict = map[string]string{"AK": "Alaska", "AL": "Alabama", "AR": "Arkansas",
	"AS": "American Samoa", "AZ": "Arizona", "CA": "California", "CO": "Colorado",
	"CT": "Connecticut", "DC": "District of Columbia", "DE": "Delaware", "FL": "Florida",
	"GA": "Georgia", "GU": "Guam", "HI": "Hawaii", "IA": "Iowa", "ID": "Idaho", "IL": "Illinois",
	"IN": "Indiana", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "MA": "Massachusetts",
	"MD": "Maryland", "ME": "Maine", "MI": "Michigan", "MN": "Minnesota", "MO": "Missouri",
	"MP": "Northern Mariana Islands", "MS": "Mississippi", "MT": "Montana", "NA": "National",
	"NC": "North Carolina", "ND": "North Dakota", "NE": "Nebraska", "NH": "New Hampshire",
	"NJ": "New Jersey", "NM": "New Mexico", "NV": "Nevada", "NY": "New York", "OH": "Ohio",
	"OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "PR": "Puerto Rico",
	"RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee",
	"TX": "Texas", "UT": "Utah", "VA": "Virginia", "VI": "Virgin Islands", "VT": "Vermont",
	"WA": "Washington", "WI": "Wisconsin", "WV": "West Virginia", "WY": "Wyoming"}

type jsonCon struct {
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
}

type payload struct {
	counter  int
	record   []string
	colMap   map[int]int
	param    jsonCon
	cord     map[string][]string
	scfFac   map[string]string
	dduFac   map[string]string
	hispN    map[string]int
	dnm      map[string]int
	genSupp  map[string]int
	genSuppN map[string]int
}

// Main Function
func main() {
	data := payload{}

	loadJSONConf()
	data.cord = loadZipCor()
	data.dduFac = loadSCFFac()
	data.scfFac = loadSCFFac()
	data.hispN = loadHist()
	data.dnm = loadDNM()
	data.genSupp = loadGenS()
	data.genSuppN = loadGenSNm()

	var (
		gophers = flag.Int("gophers", 1, "Number of gophers to run in parallel")
		outfile = flag.String("output", "output.csv", "Filename to export the CSV results")
	)
	flag.Parse()

	start := time.Now()
	r := csv.NewReader(os.Stdin)

	preprocess := make(chan payload)
	go func() {
		for c := 0; ; c++ {
			if data.counter%10000 == 0 {
				fmt.Print(">")
			}
			rec, err := r.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalln(err)
			}
			if c == 0 {
				data = setCol(data)
			} else {
				preprocess <- mapCol(data)
			}
		}
		close(preprocess)
	}()

	results := make(chan []string)

	var wg sync.WaitGroup

	wg.Add(*gophers)

	go func() {
		wg.Wait()
		close(results)
	}()

	for i := 0; i < *gophers; i++ {
		go func() {
			defer wg.Done()
			for t := range preprocess {
				results <- preprocessor(data)
			}
		}()
	}
	dumpCSV(*outfile, results)
	fmt.Printf(" Total Records: %v, Time Elapsed: %v\n", data.counter, time.Since(start))
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
		log.Fatalln("Error converting string to int", err)
	}
	return i
}
func decYr(y string) string {
	if dy, ok := yrDecDict[y]; ok {
		return dy
	}
	return y
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

func loadJSONConf() {
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

	jsonParser := json.NewDecoder(conf)
	if err = jsonParser.Decode(&param); err != nil {
		log.Fatalln("error decoding json file", err)
	}
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
		hisp[tCase(s[0])] = hisp[tCase(s[0])] + 1
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
		dnm[tCase(s[0])] = dnm[tCase(s[0])] + 1
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
		gen[adrZip] = gen[adrZip] + 1
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
		gen[fnln] = gen[fnln] + 1
	}
	return gen
}

func setCol(r payload) payload {
	c := make(map[int]int)
	for i, v := range r.record {
		switch {
		case regexp.MustCompile(`(?i)cust.+id`).MatchString(v):
			c[custID] = i
		case regexp.MustCompile(`(?i)ful.+me`).MatchString(v):
			c[fullName] = i
		case regexp.MustCompile(`(?i)fir.+me`).MatchString(v):
			c[firstName] = i
		case regexp.MustCompile(`(?i)^mi$`).MatchString(v):
			c[mi] = i
		case regexp.MustCompile(`(?i)las.+me`).MatchString(v):
			c[lastName] = i
		case regexp.MustCompile(`(?i)addr.+1`).MatchString(v):
			c[address1] = i
		case regexp.MustCompile(`(?i)addr.+2`).MatchString(v):
			c[address2] = i
		case regexp.MustCompile(`(?i)^city$`).MatchString(v):
			c[city] = i
		case regexp.MustCompile(`(?i)^state$`).MatchString(v):
			c[state] = i
		case regexp.MustCompile(`(?i)^zip$`).MatchString(v):
			c[zip] = i
		case regexp.MustCompile(`(?i)^4zip$`).MatchString(v):
			c[zip4] = i
		case regexp.MustCompile(`(?i)^zip4$`).MatchString(v):
			c[zip4] = i
		case regexp.MustCompile(`(?i)^hph$`).MatchString(v):
			c[hph] = i
		case regexp.MustCompile(`(?i)^bph$`).MatchString(v):
			c[bph] = i
		case regexp.MustCompile(`(?i)^cph$`).MatchString(v):
			c[cph] = i
		case regexp.MustCompile(`(?i)^email$`).MatchString(v):
			c[email] = i
		case regexp.MustCompile(`(?i)^vin$`).MatchString(v):
			c[vin] = i
		case regexp.MustCompile(`(?i)^year$`).MatchString(v):
			c[vyear] = i
		case regexp.MustCompile(`(?i)^vyr$`).MatchString(v):
			c[vyear] = i
		case regexp.MustCompile(`(?i)^make$`).MatchString(v):
			c[vmake] = i
		case regexp.MustCompile(`(?i)^vmk$`).MatchString(v):
			c[vmake] = i
		case regexp.MustCompile(`(?i)^model$`).MatchString(v):
			c[vmodel] = i
		case regexp.MustCompile(`(?i)^vmd$`).MatchString(v):
			c[vmodel] = i
		case regexp.MustCompile(`(?i)^DelDate$`).MatchString(v):
			c[delDate] = i
		case regexp.MustCompile(`(?i)^Date$`).MatchString(v):
			c[date] = i
		case regexp.MustCompile(`(?i)^DSF_WALK_SEQ$`).MatchString(v):
			c[dsfwalkseq] = i
		case regexp.MustCompile(`(?i)^Crrt$`).MatchString(v):
			c[crrt] = i
		case regexp.MustCompile(`(?i)^KBB$`).MatchString(v):
			c[kbb] = i
		}
	}
	r.colMap = c
	return r
}

// func mapCol(r []string, m map[int]int) []string {
// 	nr := make([]string, len(header))
// 	for i := range nr {
// 		_, ok := m[i]
// 		if ok {
// 			nr[i] = r[m[i]]
// 		}
// 	}
// 	return nr
// }

func mapCol(p payload) payload {
	nr := make([]string, len(header))
	for i := range nr {
		_, ok := p.colMap[i]
		if ok {
			nr[i] = p.record[p.colMap[i]]
		}
	}
	return p
}

func reformatPhone(p string) string {
	c := []string{"-", " ", ".", "*"}
	for _, v := range c {
		p = strings.Replace(p, v, "", -1)
	}
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

func getLatLong(cZip, rZip string) (float64, float64, float64, float64) {
	if cZip[:1] == "0" {
		cZip = cZip[1:]
	}
	if rZip[:1] == "0" {
		rZip = rZip[1:]
	}
	// Set Coordinates Feild
	recCor, ok := resc.cord[rZip]
	if !ok {
		log.Fatalln("Invalid Record Zip Code")
	}
	// Calculate Distance
	cenCor, ok := resc.cord[cZip]
	if !ok {
		log.Fatalln("Invalid Central Zip Code")
	}
	lat1, err := strconv.ParseFloat(cenCor[0], 64)
	lon1, err := strconv.ParseFloat(cenCor[1], 64)
	lat2, err := strconv.ParseFloat(recCor[0], 64)
	lon2, err := strconv.ParseFloat(recCor[1], 64)
	if err != nil {
		log.Fatalln("Error parsing Coordinates", err)
	}
	return lat1, lon1, lat2, lon2
}

func preprocessor(r []string) []string {
	for i, v := range r {
		switch i {
		case state, mi, vin:
			r[i] = uCase(v)
		case email:
			r[i] = lCase(v)
		case hph, bph, cph:
			r[i] = reformatPhone(v)
		default:
			r[i] = tCase(v)
		}
	}

	// Combine FirstName + LastName to FullName
	if r[fullName] == "" {
		r[fullName] = fmt.Sprintf("%v %v", r[firstName], r[lastName])
	}

	// Combine address1 + Address2 to AddressFull
	r[addressFull] = fmt.Sprintf("%v %v", r[address1], r[address2])

	// Set Phone field based on availability of hph, bph & cph
	if r[hph] != "" {
		r[phone] = r[hph]
	} else if r[bph] != "" {
		r[phone] = r[bph]
	} else {
		r[phone] = r[cph]
	}

	// If Zip format is 92882-2341, split to Zip & Zip4
	if len(r[zip]) == 10 {
		z := strings.Split(r[zip], "-")
		r[zip], r[zip4] = z[0], z[1]
	}

	// Set VINlen
	r[vinLen] = fmt.Sprint(len(r[vin]))

	r[zipCrrt] = fmt.Sprintf("%v%v", r[zip], r[crrt])

	// Set Radius value
	r[radius] = fmt.Sprintf("%.2f", distance(getLatLong(strconv.Itoa(param.CentZip), r[zip])))

	return r
}

func dumpCSV(o string, results <-chan []string) {
	f, err := os.Create(o)
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	w := csv.NewWriter(f)

	w.Write(header)

	for r := range results {
		if err := w.Write(r); err != nil {
			log.Fatalln(err)
		}
	}
	w.Flush()
}
