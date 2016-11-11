package main

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/vaxx99/eload/cnf"
)

type Fields struct {
	FN string
	FT string
	FS int
}

type esrec struct {
	RECNUMB    string
	STNAME     string
	LINETYPE   string
	LINECODE   string
	AREAOFFSET string
	LINECODETO string
	OLDSTATUS  string
	NEWSTATUS  string
	TALKFLAGS  string
	CAUSE      string
	ISUPCAT    string
	ENDDATE    string
	ENDTIME    string
	DURATION   string
	SUBSTO     string
	SUBSFROM   string
	REDIRSUBS  string
	CONNSUBS   string
	TALKCOMM   string
}

var wd string

type Record struct {
	Id, Sw, Hi, Na, Nb, Ds, De, Dr, Ot, It, Du string
}

type Redrec struct {
	Id string `json:"id"`
	Sw string `json:"sw"`
	Hi string `json:"hi"`
	Na string `json:"na"`
	Nb string `json:"nb"`
	Ds string `json:"ds"`
	De string `json:"de"`
	Dr string `json:"dr"`
	Ot string `json:"ot"`
	It string `json:"it"`
	Du string `json:"du"`
}

type block []Redrec

var cfg *cnf.Config

func opendb(path, name string, mod os.FileMode) *bolt.DB {
	db, err := bolt.Open(path+"/"+name, mod, nil)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func term(c *cnf.Config) {
	os.Mkdir(cfg.Path+"/bdb/"+cfg.Term, 0777)
	if time.Now().Format("20060102")[6:8] == "01" && time.Now().Format("150405")[0:2] == "06" {
		fmt.Println("current period:", time.Now().Format("200601"))
		s := `{"Path":"` + c.Path + `","Port":"` + c.Port + `","Term":"` + time.Now().Format("200601") + `"}`
		d := []byte(s)
		os.Mkdir(cfg.Path+"/bdb/"+time.Now().Format("200601"), 0777)
		err := ioutil.WriteFile("conf.json", d, 0644)
		check(err)
	}
}

func week(day string) string {
	var s string
	switch day {
	case "01", "02", "03", "04", "05", "06", "07":
		s = "week01"
	case "08", "09", "10", "11", "12", "13", "14":
		s = "week02"
	case "15", "16", "17", "18", "19", "20", "21":
		s = "week03"
	case "22", "23", "24", "25", "26", "27", "28", "29", "30", "31":
		s = "week04"
	}
	return s
}

func wize(db *bolt.DB) {
	t := time.Now()
	days := map[string]int{}
	bckn := map[string]string{}
	os.Chdir(cfg.Path + "/bdb/" + cfg.Term)
	f, _ := ioutil.ReadDir(".")
	for _, fn := range f {
		if fn.Name()[0:4] == "week" {
			wb := opendb(cfg.Path+"/bdb/"+cfg.Term+"/", fn.Name(), 0600)
			bn := bname(wb)
			for _, buckn := range bn {
				bckn[buckn] = fn.Name()
				wb.View(func(tx *bolt.Tx) error {
					// Assume bucket exists and has keys
					b := tx.Bucket([]byte(buckn))
					b.ForEach(func(k, v []byte) error {
						days["ALL"]++
						days[string(k)[0:6]]++
						days[string(k)[0:8]]++
						return nil
					})
					return nil
				})
			}
			wb.Close()
		}
	}
	db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("size"))
		for k, v := range days {
			kv := strconv.Itoa(v)
			err = bucket.Put([]byte(k), []byte(kv))
		}
		return err
	})
	db.Update(func(tx *bolt.Tx) error {
		bckt, err := tx.CreateBucketIfNotExists([]byte("buck"))
		for k, v := range bckn {
			err = bckt.Put([]byte(k), []byte(v))
		}
		return err
	})
	t1 := time.Now().Sub(t).Seconds()
	fmt.Printf("%4s %10d %10s %8.3f\n", "size:", days["ALL"], time.Now().Format("15:04:05"), t1)
}

func rset(recs []Redrec, db *bolt.DB) {
	err := db.Update(func(tx *bolt.Tx) error {
		for _, v := range recs {
			bucket, err := tx.CreateBucketIfNotExists([]byte(v.Id[0:8]))
			if err != nil {
				return err
			}
			key := v.Id + ".Sw." + v.Sw + ".Hi." + v.Hi + ".Na." + v.Na + ".Nb." + v.Nb + ".Ds." + v.Ds + ".De." + v.De +
				".Dr." + v.Dr + ".Ot." + v.Ot + ".It." + v.It + ".Du." + v.Du

			err = bucket.Put([]byte(key), []byte(v.Id[0:6]))

		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func rget(buck, key string, db *bolt.DB) {
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(buck))
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", buck)
		}

		val := bucket.Get([]byte(key))
		fmt.Println(string(val))
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
}

func fget(key string, db *bolt.DB) bool {
	var f bool
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("file"))
		if bucket == nil {
			f = false
			return nil
		}

		val := bucket.Get([]byte(key))
		if val != nil {
			f = true
		} else {
			f = false
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
	return f
}

func set(buck, key, val string, db *bolt.DB) {
	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(buck))
		if err != nil {
			return err
		}

		err = bucket.Put([]byte(key), []byte(val))
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
}

func bname(db *bolt.DB) []string {
	var bn []string
	db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if string(k)[0:4] != "file" && string(k)[0:4] != "size" {
				bn = append(bn, string(k))
			}
		}
		return nil
	})
	return bn
}

func main() {

	fmt.Println("es11 loader:", time.Now().Format("02.01.2006 15:04:05"))
	cnf.LoadConfig()
	cfg = cnf.GetConfig()
	os.Chdir(cfg.Path)
	term(cfg)
	cnf.LoadConfig()
	cfg = cnf.GetConfig()
	os.Chdir(cfg.Path + "/tmp")
	st0 := opendb(cfg.Path+"/bdb/"+cfg.Term, "stat0.db", 0666)
	defer st0.Close()
	f, _ := ioutil.ReadDir(".")
	ds := false
	for _, fn := range f {
		if ises(fn.Name()) {
			if fget(fn.Name(), st0) != true {
				var w1, w2, w3, w4 block
				ds = true
				t0 := time.Now()
				dt, rn, rp := es11(fn.Name())
				set("file", fn.Name(), rp[0].Ds[0:8], st0)

				for _, v := range rp {
					if v.Id[0:6] == cfg.Term {
						switch week(v.Id[6:8]) {
						case "week01":
							w1 = append(w1, v)
						case "week02":
							w2 = append(w2, v)
						case "week03":
							w3 = append(w3, v)
						case "week04":
							w4 = append(w4, v)
						}
					}
				}
				if len(w1) > 0 {
					wb1 := opendb(cfg.Path+"/bdb/"+cfg.Term, "week1.db", 0666)
					rset(w1, wb1)
					wb1.Close()
				}
				if len(w2) > 0 {
					wb2 := opendb(cfg.Path+"/bdb/"+cfg.Term, "week2.db", 0666)
					rset(w2, wb2)
					wb2.Close()
				}
				if len(w3) > 0 {
					wb3 := opendb(cfg.Path+"/bdb/"+cfg.Term, "week3.db", 0666)
					rset(w3, wb3)
					wb3.Close()
				}
				if len(w4) > 0 {
					wb4 := opendb(cfg.Path+"/bdb/"+cfg.Term, "week4.db", 0666)
					rset(w4, wb4)
					wb4.Close()
				}
				t1 := time.Now().Sub(t0).Seconds()
				log.Printf("%10s %15s %10d %8d %8.2f\n", dt, fn.Name(), fn.Size(), rn, t1)
			}
			os.Remove(fn.Name())
		}
	}
	if ds {
		wize(st0)
	}
	//fmt.Println("*")
}

func es2rec(erec *esrec) Redrec {
	var rec Redrec
	ds, de, dr := dates(erec)
	rec.Id = ds
	rec.Sw = Sw(erec)
	rec.Hi = Bcat(erec.ISUPCAT)
	rec.Na = erec.SUBSFROM
	rec.Nb = erec.SUBSTO
	rec.Ds = ds
	rec.De = de
	rec.Dr = erec.OLDSTATUS + erec.NEWSTATUS
	rec.It = erec.LINECODE
	rec.Ot = erec.LINECODETO
	rec.Du = dr
	return rec
}

func Sw(rec *esrec) string {
	switch rec.STNAME[0:3] {
	case "023":
		return "3846"
	case "024":
		return "3847"
	case "025":
		return "3856"
	case "026":
		return "3853"
	case "027":
		return "3857"
	case "028":
		return "3852"
	case "076":
		return "3859"
	case "088":
		return "3844"
	case "101":
		return "3850"
	case "102":
		return "3841"
	case "431":
		return "3855"
	}
	return "38xx"
}

func Bcat(sc string) string {
	cat, _ := strconv.ParseInt(sc, 16, 64)
	switch cat {
	case 10:
		return "1"
	case 11:
		return "4"
	case 12:
		return "8"
	case 13:
		return "TC"
	case 15:
		return "9"
	case 224:
		return "0"
	case 225:
		return "2"
	case 226:
		return "5"
	case 227:
		return "7"
	case 228:
		return "3"
	case 229:
		return "6"
	}
	return "XX"
}

func ises(fn string) bool {
	f, _ := os.Open(fn)
	v, _ := Read(f, 2)
	a := int(v[0])
	b := 1900 + int(v[1])
	defer f.Close()
	if a == 3 && b == time.Now().Add(-24*time.Hour).Year() {
		return true
	}
	return false
}

func es11(fn string) (string, uint32, block) {

	var Rec block
	f, e := Open(fn)
	defer f.Close()
	dt, rn, hb, rb, fd, e := head(f)

	if e != nil {
		panic(e)
	}
	f, e = Open(fn)
	if e != nil {
		panic(e)
	}
	defer f.Close()
	v, _ := Read(f, int(hb))
	var rec esrec

	recVal := reflect.ValueOf(&rec).Elem()
	for i := 0; i < int(rn); i++ {
		v, e = Read(f, int(rb))
		if e != nil {
			panic(e)
		}
		sb := 1
		eb := 1
		for a, b := range fd {
			eb = sb + b.FS
			s := strings.Replace(string(v[sb:eb]), " ", "", -1)
			recVal.Field(a).SetString(s)
			sb = eb
		}
		Rec = append(Rec, es2rec(&rec))
	}
	return dt, rn, Rec
}

func dates(rec *esrec) (string, string, string) {
	dt := rec.ENDDATE
	tm := rec.ENDTIME
	de := dt + " " + tm
	dr := s2i(rec.DURATION)
	te, _ := time.Parse("20060102 15:04:05", de)
	ts := te.Add(time.Second * time.Duration(dr) * -1)
	de = te.Format("20060102150405")
	ds := ts.Format("20060102150405")
	return ds, de, strconv.Itoa(dr)
}

func s2i(s string) int {
	s = strings.Replace(s, " ", "", -1)
	a, _ := strconv.Atoi(s)
	return a
}

func head(f *os.File) (string, uint32, uint16, uint16, []Fields, error) {
	v, e := Read(f, 1)
	//Date modified 3
	v, e = Read(f, 3)
	//YY
	yy := strconv.Itoa(1900 + int(v[0]))
	mm := dd(int(v[1]))
	dd := dd(int(v[2]))
	dt := dd + "." + mm + "." + yy
	//Number of records 4-7 (4)
	v, e = Read(f, 4)
	rn := binary.LittleEndian.Uint32(v)
	//Number of bytes in header 8-9 (2)
	v, e = Read(f, 2)
	hb := binary.LittleEndian.Uint16(v)
	//Number of bytes in the record 10-11 (2)
	v, e = Read(f, 2)
	rb := binary.LittleEndian.Uint16(v)
	//12-14 	3 bytes 	Reserved bytes.
	v, e = Read(f, 3)
	//15-27 	13 bytes 	Reserved for dBASE III PLUS on a LAN.
	v, e = Read(f, 13)
	//28-31 	4 bytes 	Reserved bytes.
	v, e = Read(f, 4)
	//32-n 	32 bytes 	Field descriptor array (the structure of this array is each shown below)
	var br int
	var fld []Fields

	for br != 13 {
		v, e = Read(f, 32)
		br = int(v[0])
		if br != 13 {
			fld = append(fld, Fields{string(v[0:11]), string(v[11:12]), int(v[16])})
		}
	}
	f.Seek(0, 0)
	return dt, rn, hb, rb, fld, e
}

//Open file
func Open(fn string) (*os.File, error) {
	file, e := os.Open(fn)
	if e != nil {
		log.Println("File open error:", e)
	}
	return file, e
}

//Read file
func Read(file *os.File, bt int) ([]byte, error) {
	data := make([]byte, bt)
	_, e := file.Read(data)
	if e != nil {
		log.Println("File open error:", e)
	}
	return data, e
}

func dd(d int) string {
	if d < 10 {
		return "0" + strconv.Itoa(d)
	}
	return strconv.Itoa(d)
}
