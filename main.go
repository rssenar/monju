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
	lsdYear              // 45
	lsdMonth             // 46
	lsdDay               // 47
	misc1                // 48
	misc2                // 49
	misc3                // 50
)

// Set Header row
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
	"Lsd_Year",
	"Lsd_Month",
	"Lsd_Day",
	"Misc1",
	"Misc2",
	"Misc",
}

// YearDecodeDict is a map of 2-Digit abbreviated Years
var yrDecDict = map[string]string{"0": "2000",
	"1": "2001", "2": "2002", "3": "2003", "4": "2004", "5": "2005",
	"6": "2006", "7": "2007", "8": "2008", "9": "2009", "10": "2010",
	"11": "2011", "12": "2012", "13": "2013", "14": "2014", "15": "2015",
	"16": "2016", "17": "2017", "18": "2018", "19": "2019", "20": "2020",
	"40": "1940", "41": "1941", "42": "1942", "43": "1943", "44": "1944",
	"45": "1945", "46": "1946", "47": "1947", "48": "1948", "49": "1949",
	"50": "1950", "51": "1951", "52": "1952", "53": "1953", "54": "1954",
	"55": "1955", "56": "1956", "57": "1957", "58": "1958", "59": "1959",
	"60": "1960", "61": "1961", "62": "1962", "63": "1963", "64": "1964",
	"65": "1965", "66": "1966", "67": "1967", "68": "1968", "69": "1969",
	"70": "1970", "71": "1971", "72": "1972", "73": "1973", "74": "1974",
	"75": "1975", "76": "1976", "77": "1977", "78": "1978", "79": "1979",
	"80": "1980", "81": "1981", "82": "1982", "83": "1983", "84": "1984",
	"85": "1985", "86": "1986", "87": "1987", "88": "1988", "89": "1989",
	"90": "1990", "91": "1991", "92": "1992", "93": "1993", "94": "1994",
	"95": "1995", "96": "1996", "97": "1997", "98": "1998", "99": "1999"}

// UsStatesDict is a map of 2-Digit abbreviated US States
var usStDict = map[string]string{"AK": "Alaska", "AL": "Alabama",
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
}

type payload struct {
	counter int
	record  []string
	param   initConfig
}

type resources struct {
	cord   map[string][]string
	scfFac map[string]string
	dduFac map[string]string
	hist   map[string]int
	dnm    map[string]int
	genS   map[string]int
	genSNm map[string]int
}

func main() {
	start := time.Now()
	var (
		counter int
		gophers = flag.Int("gophers", 1, "Workers to run in parallel")
		outfile = fmt.Sprintf("%v_output.csv", readDir()[:len(readDir())-4])
		colMap  map[int]int
	)
	flag.Parse()

	file, err := os.Open(readDir())
	if err != nil {
		log.Fatalln("error opening file", err)
	}
	reader := csv.NewReader(file)

	resource := resources{
		cord:   loadZipCor(),
		scfFac: loadSCFFac(),
		dduFac: loadDDUFac(),
		hist:   loadHist(),
		dnm:    loadDNM(),
		genS:   loadGenS(),
		genSNm: loadGenSNm(),
	}

	tasks := make(chan payload)
	go func() {
		for i := 0; ; i++ {
			counter = i
			if i%10000 == 0 {
				fmt.Print(">")
			}
			row, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalln("Error reading source", err)
			}
			if i == 0 {
				colMap = setCol(payload{
					counter: i,
					record:  row,
				})
			} else {
				tasks <- mapCol(payload{
					counter: i,
					record:  row,
					param:   loadConfig(),
				}, colMap)
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
				r := process(t, resource)
				results <- r
			}
		}()
	}
	outputCSV(outfile, results)
	fmt.Printf(" Elapsed Time: %v, Total: %v\n", time.Since(start), counter)
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
	if cZip[:1] == "0" {
		cZip = cZip[1:]
	}
	if rZip[:1] == "0" {
		rZip = rZip[1:]
	}
	// Set Coordinates Feild
	recCor, ok := res.cord[rZip]
	if !ok {
		log.Fatalln("Invalid Record Zip Code")
	}
	// Calculate Distance
	cenCor, ok := res.cord[cZip]
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

func parseDate(d string) (string, string, string, string) {
	if d != "" {
		formats := []string{"1/2/2006", "1-2-2006", "1/2/06", "1-2-06", "2006/1/2", "2006-1-2"}
		for _, f := range formats {
			if t, err := time.Parse(f, d); err == nil {
				nDate := fmt.Sprintf("%v/%v/%v", strconv.Itoa(t.Year()), strconv.Itoa(int(t.Month())), strconv.Itoa(t.Day()))
				return nDate, strconv.Itoa(t.Year()), strconv.Itoa(int(t.Month())), strconv.Itoa(t.Day())
			}
		}
	}
	return "", "", "", ""
}

func parseFullName(fn string) (string, string, string) {
	if len(strings.Split(fn, " ")) == 2 {
		name := strings.Split(fn, " ")
		return name[0], "", name[1]
	} else if len(strings.Split(fn, " ")) == 3 {
		name := strings.Split(fn, " ")
		return name[0], name[1], name[2]
	}
	return "", "", ""
}

func readDir() string {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal("Error reading Directory", err)
	}
	var f []string
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".csv" {
			f = append(f, file.Name())
		}
	}
	if len(f) > 1 {
		log.Fatalln("Directory must contain single .csv file")
	}
	return f[0]
}

func setCol(r payload) map[int]int {
	c := make(map[int]int)
	for i, v := range r.record {
		switch {
		case regexp.MustCompile(`(?i)cust.+id`).MatchString(tCase(v)):
			c[custID] = i
		case regexp.MustCompile(`(?i)ful.+me`).MatchString(tCase(v)):
			c[fullName] = i
		case regexp.MustCompile(`(?i)fir.+me`).MatchString(tCase(v)):
			c[firstName] = i
		case regexp.MustCompile(`(?i)^mi$`).MatchString(tCase(v)):
			c[mi] = i
		case regexp.MustCompile(`(?i)las.+me`).MatchString(tCase(v)):
			c[lastName] = i
		case regexp.MustCompile(`(?i)addr.+1`).MatchString(tCase(v)):
			c[address1] = i
		case regexp.MustCompile(`(?i)addr.+2`).MatchString(tCase(v)):
			c[address2] = i
		case regexp.MustCompile(`(?i)^city$`).MatchString(tCase(v)):
			c[city] = i
		case regexp.MustCompile(`(?i)^state$`).MatchString(tCase(v)):
			c[state] = i
		case regexp.MustCompile(`(?i)^zip$`).MatchString(tCase(v)):
			c[zip] = i
		case regexp.MustCompile(`(?i)^4zip$`).MatchString(tCase(v)):
			c[zip4] = i
		case regexp.MustCompile(`(?i)^zip4$`).MatchString(tCase(v)):
			c[zip4] = i
		case regexp.MustCompile(`(?i)^hph$`).MatchString(tCase(v)):
			c[hph] = i
		case regexp.MustCompile(`(?i)^bph$`).MatchString(tCase(v)):
			c[bph] = i
		case regexp.MustCompile(`(?i)^cph$`).MatchString(tCase(v)):
			c[cph] = i
		case regexp.MustCompile(`(?i)^email$`).MatchString(tCase(v)):
			c[email] = i
		case regexp.MustCompile(`(?i)^vin$`).MatchString(tCase(v)):
			c[vin] = i
		case regexp.MustCompile(`(?i)^year$`).MatchString(tCase(v)):
			c[vyear] = i
		case regexp.MustCompile(`(?i)^vyr$`).MatchString(tCase(v)):
			c[vyear] = i
		case regexp.MustCompile(`(?i)^make$`).MatchString(tCase(v)):
			c[vmake] = i
		case regexp.MustCompile(`(?i)^vmk$`).MatchString(tCase(v)):
			c[vmake] = i
		case regexp.MustCompile(`(?i)^model$`).MatchString(tCase(v)):
			c[vmodel] = i
		case regexp.MustCompile(`(?i)^vmd$`).MatchString(tCase(v)):
			c[vmodel] = i
		case regexp.MustCompile(`(?i)^DelDate$`).MatchString(tCase(v)):
			c[delDate] = i
		case regexp.MustCompile(`(?i)^Date$`).MatchString(tCase(v)):
			c[date] = i
		case regexp.MustCompile(`(?i)^DSF_WALK_SEQ$`).MatchString(tCase(v)):
			c[dsfwalkseq] = i
		case regexp.MustCompile(`(?i)^Crrt$`).MatchString(tCase(v)):
			c[crrt] = i
		case regexp.MustCompile(`(?i)^KBB$`).MatchString(tCase(v)):
			c[kbb] = i
		}
	}
	return c
}

func mapCol(r payload, m map[int]int) payload {
	nr := make([]string, len(header))
	for i := range nr {
		_, ok := m[i]
		if ok {
			nr[i] = r.record[m[i]]
		}
	}
	r.record = nr
	return r
}

func process(pay payload, res resources) payload {
	for i, v := range pay.record {
		switch i {
		case state, mi, vin:
			pay.record[i] = uCase(v)
		case email:
			pay.record[i] = lCase(v)
		case hph, bph, cph:
			pay.record[i] = reformatPhone(v)
		default:
			pay.record[i] = tCase(v)
		}
	}
	// Parse FullName if FirstName & LastName == ""
	if pay.record[fullName] != "" && pay.record[firstName] == "" && pay.record[lastName] == "" {
		pay.record[firstName], pay.record[mi], pay.record[lastName] = parseFullName(pay.record[fullName])
	}
	// Combine FirstName + LastName to FullName
	if pay.record[fullName] == "" {
		pay.record[fullName] = fmt.Sprintf("%v %v", pay.record[firstName], pay.record[lastName])
	}
	// Combine address1 + Address2 to AddressFull
	pay.record[addressFull] = fmt.Sprintf("%v %v", pay.record[address1], pay.record[address2])
	// Set Phone field based on availability of hph, bph & cph
	if pay.record[hph] != "" {
		pay.record[phone] = pay.record[hph]
	} else if pay.record[bph] != "" {
		pay.record[phone] = pay.record[bph]
	} else {
		pay.record[phone] = pay.record[cph]
	}
	// If Zip format is 92882-2341, split to Zip & Zip4
	if len(pay.record[zip]) == 10 {
		z := strings.Split(pay.record[zip], "-")
		pay.record[zip], pay.record[zip4] = z[0], z[1]
	}
	// Set VINlen
	pay.record[vinLen] = fmt.Sprint(len(pay.record[vin]))
	// Set ZipCrrt
	pay.record[zipCrrt] = fmt.Sprintf("%v%v", pay.record[zip], pay.record[crrt])
	// Set Radius(miles) based on Central Zip and Row Zip
	pay.record[radius] = fmt.Sprintf("%.2f", distance(getLatLong(strconv.Itoa(pay.param.CentZip), pay.record[zip], res)))
	// Set Coordinte value
	_, _, vlat1, vlon2 := getLatLong(strconv.Itoa(pay.param.CentZip), pay.record[zip], res)
	pay.record[coordinates] = fmt.Sprintf("%v,%v", vlat1, vlon2)
	// Set DelDate, Date, Dld_Year, Dld_Month, Dld_Day
	pay.record[delDate], pay.record[dldYear], pay.record[dldMonth], pay.record[dldDay] = parseDate(pay.record[delDate])
	pay.record[date], pay.record[lsdYear], pay.record[lsdMonth], pay.record[lsdDay] = parseDate(pay.record[date])
	// Set Extended State Value
	if exstate, ok := usStDict[pay.record[state]]; ok {
		pay.record[expandedState] = exstate
	}
	// Set SCF value
	pay.record[scf] = setSCF(pay.record[zip])
	// Set DDU Faculity
	if ddufac, ok := res.dduFac[pay.record[zip]]; ok {
		pay.record[dduFacility] = ddufac
	}
	// Set SCF Faculity
	if scffac, ok := res.scfFac[pay.record[scf]]; ok {
		pay.record[scf3dFacility] = scffac
	}
	// Set Ethnicity
	if _, ok := res.hist[pay.record[lastName]]; ok {
		pay.record[ethnicity] = "Hisp"
	}
	// Set Vendor
	pay.record[vendor] = pay.param.Vendor

	return pay
}

func outputCSV(out string, results <-chan payload) {
	f, err := os.Create(out)
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write(header)

	for r := range results {
		if err := w.Write(r.record); err != nil {
			log.Fatalln(err)
		}
	}
	w.Flush()
}
