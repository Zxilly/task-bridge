package storage

import "io/fs"

func subMigrations() (fs.FS, error) { return fs.Sub(migrations, "migrations") }
