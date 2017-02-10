package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
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

type payload struct {
	counter int
	record  []string
}

func main() {
	start := time.Now()
	var (
		gophers = flag.Int("gophers", 1, "Workers to run in parallel")
		outfile = flag.String("output", "output.csv", "Export CSV name")
	)
	flag.Parse()

	r := csv.NewReader(os.Stdin)

	var colMap map[int]int

	tasks := make(chan payload)
	go func() {
		for i := 0; ; i++ {
			if i%10000 == 0 {
				fmt.Print(">")
			}
			row, err := r.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalln(err)
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
				r := process(t)
				results <- r
			}
		}()
	}
	dumpCSV(*outfile, results)
	fmt.Printf("Elapsed Time: %v\n", time.Since(start).Seconds())
}

func setCol(r payload) map[int]int {
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

func process(r payload) payload {
	r.record[custID] = strconv.Itoa(r.counter)
	r.record[firstName] = "Richard"
	r.record[lastName] = "Senar"
	return r
}

func dumpCSV(out string, results <-chan payload) {
	f, err := os.Create(out)
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()
	w := csv.NewWriter(os.Stdout)
	w.Write(header)

	for r := range results {
		if err := w.Write(r.record); err != nil {
			log.Fatalln(err)
		}
	}
	w.Flush()
}
