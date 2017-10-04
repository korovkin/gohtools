package gohtools

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	_ "github.com/lib/pq"
)

type StorePqTag struct {
	Db                 *sql.DB
	InsertStmt         *sql.Stmt
	GetStmt            *sql.Stmt
	IterateStmt        *sql.Stmt
	IterateAllStmt     *sql.Stmt
	IterateByPrefixASC *sql.Stmt
	IterateByPrefixDSC *sql.Stmt
	DeleteStmt         *sql.Stmt
	DeleteStmtTag      *sql.Stmt
	DeleteAllStmt      *sql.Stmt
	CountAllStmt       *sql.Stmt
}

func NewStorePqTag(name string, connection string, db *sql.DB) (*StorePqTag, error) {
	log.Println("NewStorePqTag:", name)
	now := time.Now()
	defer func() {
		log.Println("NewStorePqTag:", name, "dt:", time.Since(now))
	}()
	var err error
	store := StorePqTag{}
	tableName := "kv_" + name

	if db == nil {
		db, err = sql.Open("postgres", connection)
		CheckNotFatal(err)
	}
	if err != nil {
		return nil, err
	}

	store.Db = db

	_, err = store.Db.Exec("CREATE TABLE IF NOT EXISTS " + tableName + " (K text primary key, V text, T text);")
	CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	_, err = store.Db.Exec("CREATE INDEX IF NOT EXISTS KV_K_" + name + " ON " + tableName + " (K, T);")
	CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	_, err = store.Db.Exec("CREATE INDEX IF NOT EXISTS KV_T_" + name + " ON " + tableName + " (T, K);")
	CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.InsertStmt, err = store.Db.Prepare("INSERT INTO " + tableName + "(K, V, T) VALUES($1, $2, $3) ON CONFLICT (K) DO UPDATE SET V=$2, T=$3")
	CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.GetStmt, err = store.Db.Prepare("SELECT K, V, T FROM " + tableName + " WHERE K=$1")
	CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.IterateStmt, err = store.Db.Prepare("SELECT K, V, T FROM " + tableName + " WHERE K<=$1 ORDER BY K DESC LIMIT $2")
	CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.IterateAllStmt, err = store.Db.Prepare("SELECT K, V, T FROM " + tableName + " ORDER BY K")
	CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.IterateByPrefixASC, err = store.Db.Prepare(
		"SELECT K, V, T FROM " + tableName +
			" WHERE K > $1" +
			" ORDER BY K ASC " +
			" LIMIT $2")
	CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.IterateByPrefixDSC, err = store.Db.Prepare(
		"SELECT K, V, T FROM " + tableName +
			" WHERE K < $1" +
			" ORDER BY K DESC" +
			" LIMIT $2")
	CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.DeleteStmt, err = store.Db.Prepare("DELETE FROM " + tableName + " WHERE K=$1")
	CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.DeleteStmtTag, err = store.Db.Prepare("DELETE FROM " + tableName + " WHERE T=$1")
	CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.DeleteAllStmt, err = store.Db.Prepare("DELETE FROM " + tableName)
	CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.CountAllStmt, err = store.Db.Prepare("SELECT COUNT(K), MIN(K), MAX(K) FROM " + tableName)
	CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	return &store, nil
}

func (s *StorePqTag) Close() {
	s.InsertStmt.Close()
	s.GetStmt.Close()
	s.IterateStmt.Close()
	s.IterateByPrefixASC.Close()
	s.IterateByPrefixDSC.Close()
	s.DeleteStmt.Close()
	s.DeleteStmtTag.Close()
	s.DeleteAllStmt.Close()
	s.CountAllStmt.Close()
	s.Db.Close()
}

func (s *StorePqTag) AddValueKVT(k string, v string, t string) error {
	_, err := s.InsertStmt.Exec(k, v, t)
	CheckNotFatal(err)
	return err
}

func (s *StorePqTag) AddValueKV(k string, v string) error {
	_, err := s.InsertStmt.Exec(k, v, "")
	CheckNotFatal(err)
	return err
}

func (s *StorePqTag) DeleteValue(k string) error {
	_, err := s.DeleteStmt.Exec(k)
	CheckNotFatal(err)
	return err
}

func (s *StorePqTag) DeleteAllWithTag(t string) error {
	_, err := s.DeleteStmtTag.Exec(t)
	CheckNotFatal(err)
	return err
}

func (s *StorePqTag) DeleteAll() error {
	_, err := s.DeleteAllStmt.Exec()
	CheckNotFatal(err)
	return err
}

func (s *StorePqTag) AddValueAsJSON(k string, t string, o interface{}) error {
	b, err := json.Marshal(o)
	CheckNotFatal(err)

	if err == nil {
		_, err = s.InsertStmt.Exec(k, b, t)
		CheckNotFatal(err)
		return err
	}

	return err
}

func (s *StorePqTag) GetValueAsJSON(k string, o interface{}) error {
	res, err := s.GetStmt.Query(k)
	CheckNotFatal(err)
	if err != nil {
		return err
	}
	defer res.Close()

	for res.Next() {
		var k string
		var v string
		var t string
		err = res.Scan(&k, &v, &t)
		CheckNotFatal(err)

		if err != nil {
			continue
		}

		err = json.Unmarshal([]byte(v), o)
		CheckNotFatal(err)

		// k is a primary key
		break
	}

	return err
}

func (s *StorePqTag) CountAll() (int64, string, string) {
	res, err := s.CountAllStmt.Query()
	CheckNotFatal(err)

	if err != nil {
		return -1, "", ""
	}

	defer res.Close()
	for res.Next() {
		var count int64
		var min string
		var max string
		res.Scan(&count, &min, &max)
		if err != nil {
			return 0, "", ""
		}
		return count, min, max
	}
	return -1, "", ""

}

func (s *StorePqTag) IterateByKeyPrefixASC(
	keyPrefix string,
	limit int,
	block func(k *string, t *string, v *string, stop *bool)) error {
	var err error = nil
	var res *sql.Rows = nil

	res, err = s.IterateByPrefixASC.Query(keyPrefix, limit)
	CheckNotFatal(err)

	if err != nil {
		return err
	}

	defer res.Close()
	stop := false
	for res.Next() && false == stop {
		var k string
		var v string
		var t string
		err = res.Scan(&k, &v, &t)
		CheckNotFatal(err)

		if err != nil {
			break
		}

		block(&k, &t, &v, &stop)
	}

	return err
}

func (s *StorePqTag) IterateByKeyPrefixDESC(
	keyPrefix string,
	limit int,
	block func(k *string, t *string, v *string, stop *bool)) error {
	var err error = nil
	var res *sql.Rows = nil

	res, err = s.IterateByPrefixDSC.Query(keyPrefix, limit)
	CheckNotFatal(err)

	if err != nil {
		return err
	}

	defer res.Close()
	stop := false
	for res.Next() && false == stop {
		var k string
		var v string
		var t string
		err = res.Scan(&k, &v, &t)
		CheckNotFatal(err)

		if err != nil {
			break
		}

		block(&k, &t, &v, &stop)
	}

	return err
}

func (s *StorePqTag) IterateAll(
	o interface{},
	block func(k string, stop *bool)) {
	var err error = nil
	var res *sql.Rows = nil
	res, err = s.IterateAllStmt.Query()
	CheckNotFatal(err)
	if err != nil {
		return
	}
	defer res.Close()

	stop := false
	for res.Next() {
		var k string
		var v string
		err = res.Scan(&k, &v)
		CheckNotFatal(err)
		if err != nil {
			continue
		}

		err = json.Unmarshal([]byte(v), &o)
		CheckNotFatal(err)
		if err != nil {
			continue
		}
		block(k, &stop)
		if stop {
			break
		}
	}
}

func (s *StorePqTag) GetValue(k string) *string {
	res, err := s.GetStmt.Query(k)
	CheckNotFatal(err)
	if err != nil {
		return nil
	}
	defer res.Close()
	for res.Next() {
		var k string
		var v string
		var t string
		err = res.Scan(&k, &v, &t)
		CheckNotFatal(err)
		if err != nil {
			return nil
		}
		return &v
	}
	return nil
}
