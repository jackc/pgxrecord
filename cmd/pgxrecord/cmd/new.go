package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgxrecord/gen"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// newCmd represents the new command
var newCmd = &cobra.Command{
	Use:   "new TABLE_NAME",
	Short: "Create a new pgxrecord description from a database table",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		conn, err := pgx.Connect(ctx, viper.GetString("database_url"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect to database: %v\n", err)
			os.Exit(1)
		}
		defer conn.Close(ctx)

		tableName := args[0]

		table, err := gen.NewTableFromPgCatalog(context.Background(), conn, viper.GetString("schema"), tableName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get table structure: %v\n", err)
			os.Exit(1)
		}

		table.PackageName = viper.GetString("package")

		table.StructName = viper.GetString("struct")
		if table.StructName == "" {
			table.StructName = strings.TrimSuffix(gen.ToUpperCamelCase(table.TableName), "s")
		}

		table.ReceiverName = viper.GetString("receiver")

		j, _ := json.MarshalIndent(table, "", "  ")
		fmt.Println(string(j))
	},
}

func init() {
	rootCmd.AddCommand(newCmd)

	newCmd.Flags().StringP("database-url", "d", "", "Database URL or DSN")
	viper.BindPFlag("database_url", newCmd.Flags().Lookup("database-url"))

	defaultPackage := ""
	if workingDir, err := os.Getwd(); err == nil && len(workingDir) > 0 {
		defaultPackage = path.Base(workingDir)
	}
	newCmd.Flags().String("package", defaultPackage, "package name")
	viper.BindPFlag("package", newCmd.Flags().Lookup("package"))

	newCmd.Flags().String("schema", "", "schema name")
	viper.BindPFlag("schema", newCmd.Flags().Lookup("schema"))

	newCmd.Flags().String("struct", "", "struct name")
	viper.BindPFlag("struct", newCmd.Flags().Lookup("struct"))

	newCmd.Flags().String("receiver", "r", "receiver name")
	viper.BindPFlag("receiver", newCmd.Flags().Lookup("receiver"))
}
