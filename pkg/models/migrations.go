package models

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type migration struct {
	apply  func(db *gorm.DB) error
	revert func(db *gorm.DB) error
}

type MigrationConfig struct {
	DBPath  string
	ToIndex *int
}

// Operation types
type operation int

const (
	apply  operation = 0
	revert           = 1
)

var migrations = []migration{
	// 001
	{
		apply: func(db *gorm.DB) error {
			var tables = []interface{}{&Actor{}, &Post{}, &SubState{}, &Like{}}

			for _, table := range tables {
				if !db.Migrator().HasTable(table) {
					err := db.Migrator().CreateTable(table)
					if err != nil {
						return err
					}
				}
			}

			return nil
		},
		revert: func(db *gorm.DB) error {
			var tables = []interface{}{&Actor{}, &Post{}, &SubState{}, &Like{}}

			for _, table := range tables {
				err := db.Migrator().DropTable(table)
				if err != nil {
					return err
				}
			}

			return nil
		},
	},
}

func executeOperation(config *MigrationConfig, op operation) *gorm.DB {
	db, err := gorm.Open(
		sqlite.Open(config.DBPath),
		&gorm.Config{},
	)

	if err != nil {
		panic("failed to connect database")
	}

	toIndex := len(migrations)
	if config.ToIndex != nil {
		toIndex = *config.ToIndex
	}

	switch op {
	case apply:
		for i := 0; i < toIndex; i++ {
			migrations[i].apply(db)
		}
	case revert:
		for i := toIndex - 1; i >= 0; i-- {
			migrations[i].revert(db)
		}
	}

	return db
}

func Migrate(config *MigrationConfig) *gorm.DB {
	return executeOperation(config, apply)
}

func Revert(config *MigrationConfig) *gorm.DB {
	return executeOperation(config, revert)
}
