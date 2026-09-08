package gorm_test

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/testutil"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type product struct {
	Optional sql.NullString
	ID       int64 `gorm:"primaryKey;autoIncrement:false"`
	Name     string
	Price    int64
	Active   bool
	Payload  []byte `gorm:"type:blob"`
}

type productWithNote struct {
	product
	Note string
}

func (productWithNote) TableName() string { return "products" }

func TestIntegrationGORM(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared=%v", prepared), func(t *testing.T) {
			pool := testutil.Open(t)
			db, err := gorm.Open(postgres.New(postgres.Config{Conn: pool}), &gorm.Config{
				PrepareStmt: prepared, Logger: logger.Default.LogMode(logger.Silent),
			})
			if err != nil {
				t.Fatal(err)
			}
			// Exercise actual ORM-generated DDL, without claiming PostgreSQL catalog
			// introspection or AutoMigrate compatibility.
			if err := db.Migrator().CreateTable(&product{}); err != nil {
				t.Fatal(err)
			}
			if err := db.Migrator().AddColumn(&productWithNote{}, "Note"); err != nil {
				t.Fatal(err)
			}
			item := product{Optional: sql.NullString{String: "optional", Valid: true}, ID: 1, Name: "O'Reilly ? $2", Price: 120, Active: true, Payload: []byte{0, 1, 255}}
			if r := db.Create(&item); r.Error != nil || r.RowsAffected != 1 {
				t.Fatalf("create: rows=%d err=%v", r.RowsAffected, r.Error)
			}
			var got product
			if err := db.First(&got, "id = ?", 1).Error; err != nil {
				t.Fatal(err)
			}
			if got.Optional != item.Optional || got.ID != item.ID || got.Name != item.Name || got.Price != item.Price || !got.Active || !bytes.Equal(got.Payload, item.Payload) {
				t.Fatalf("round trip: %#v", got)
			}
			if r := db.Model(&product{}).Where("id = ?", 1).Updates(map[string]any{"price": 0, "active": false}); r.Error != nil || r.RowsAffected != 1 {
				t.Fatalf("update: %v rows=%d", r.Error, r.RowsAffected)
			}
			got = product{}
			if err := db.First(&got, 1).Error; err != nil || got.Price != 0 || got.Active {
				t.Fatalf("zero values: %#v err=%v", got, err)
			}
			rollback := errors.New("rollback requested")
			err = db.Transaction(func(tx *gorm.DB) error {
				if err := tx.Create(&product{ID: 2, Name: "rolled back"}).Error; err != nil {
					return err
				}
				return rollback
			})
			if !errors.Is(err, rollback) {
				t.Fatalf("rollback: %v", err)
			}
			if err := db.First(&product{}, 2).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
				t.Fatalf("rollback leaked row: %v", err)
			}
			if err := db.Transaction(func(tx *gorm.DB) error { return tx.Create(&product{ID: 3, Name: "committed"}).Error }); err != nil {
				t.Fatal(err)
			}
			if err := db.First(&product{}, 3).Error; err != nil {
				t.Fatalf("commit: %v", err)
			}
			if r := db.Delete(&product{}, 1); r.Error != nil || r.RowsAffected != 1 {
				t.Fatalf("delete: %v rows=%d", r.Error, r.RowsAffected)
			}
			if err := db.First(&product{}, 1).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
				t.Fatalf("delete retained row: %v", err)
			}
		})
	}
}
