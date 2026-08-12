package bootstrap

import (
	"fmt"
	stdlog "log"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/cmd/flags"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	log "github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

func InitDB() {
	logLevel := logger.Silent
	if flags.Debug || flags.Dev {
		logLevel = logger.Info
	}
	newLogger := logger.New(
		stdlog.New(log.StandardLogger().Out, "\r\n", stdlog.LstdFlags),
		logger.Config{
			SlowThreshold:             time.Second,
			LogLevel:                  logLevel,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)
	gormConfig := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			TablePrefix: conf.Conf.Database.TablePrefix,
		},
		Logger: newLogger,
	}
	var dB *gorm.DB
	var err error
	if flags.Dev {
		dB, err = gorm.Open(openSQLite("file::memory:?cache=shared"), gormConfig)
		conf.Conf.Database.Type = "sqlite3"
	} else {
		database := conf.Conf.Database
		switch database.Type {
		case "sqlite3":
			{
				if !(strings.HasSuffix(database.DBFile, ".db") && len(database.DBFile) > 3) {
					log.Fatalf("db name error.")
				}
				dB, err = gorm.Open(openSQLite(database.DBFile), gormConfig)
			}
		case "mysql":
			{
				dsn := database.DSN
				if dsn == "" {
					//[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
					dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&tls=%s",
						database.User, database.Password, database.Host, database.Port, database.Name, database.SSLMode)
				}
				dB, err = gorm.Open(mysql.Open(dsn), gormConfig)
			}
		case "postgres":
			{
				dsn := database.DSN
				if dsn == "" {
					if database.Password != "" {
						dsn = fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=%s TimeZone=Asia/Shanghai",
							database.Host, database.User, database.Password, database.Name, database.Port, database.SSLMode)
					} else {
						dsn = fmt.Sprintf("host=%s user=%s dbname=%s port=%d sslmode=%s TimeZone=Asia/Shanghai",
							database.Host, database.User, database.Name, database.Port, database.SSLMode)
					}
				}
				dB, err = gorm.Open(postgres.Open(dsn), gormConfig)
			}
		default:
			log.Fatalf("not supported database type: %s", database.Type)
		}
	}
	if err != nil {
		log.Fatalf("failed to connect database:%s", err.Error())
	}
	if !flags.Dev && conf.Conf.Database.Type == "postgres" {
		sqlDB, sqlErr := dB.DB()
		if sqlErr != nil {
			log.Fatalf("failed to configure database pool: %s", sqlErr.Error())
		}
		maxOpen := conf.Conf.Database.MaxOpenConns
		if maxOpen <= 0 {
			maxOpen = 20
		}
		maxIdle := conf.Conf.Database.MaxIdleConns
		if maxIdle <= 0 || maxIdle > maxOpen {
			maxIdle = maxOpen / 2
			if maxIdle < 1 {
				maxIdle = 1
			}
		}
		lifetimeMinutes := conf.Conf.Database.ConnMaxLifetimeMinutes
		if lifetimeMinutes <= 0 {
			lifetimeMinutes = 30
		}
		sqlDB.SetMaxOpenConns(maxOpen)
		sqlDB.SetMaxIdleConns(maxIdle)
		sqlDB.SetConnMaxLifetime(time.Duration(lifetimeMinutes) * time.Minute)
	}
	db.Init(dB)
}
