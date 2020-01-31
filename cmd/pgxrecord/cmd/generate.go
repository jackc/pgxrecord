package cmd

import (
	"context"
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

// generateCmd represents the generate command
var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate a pgxrecord code from a database",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		conn, err := pgx.Connect(ctx, viper.GetString("database_url"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect to database: %v\n", err)
			os.Exit(1)
		}
		defer conn.Close(ctx)

		table, err := gen.NewTableFromPgCatalog(context.Background(), conn, viper.GetString("schema"), viper.GetString("table"))
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

		table.Generate(os.Stdout)
	},
}

func init() {
	rootCmd.AddCommand(generateCmd)

	generateCmd.Flags().StringP("database-url", "d", "", "Database URL or DSN")
	viper.BindPFlag("database_url", generateCmd.Flags().Lookup("database-url"))

	defaultPackage := ""
	if workingDir, err := os.Getwd(); err == nil && len(workingDir) > 0 {
		defaultPackage = path.Base(workingDir)
	}
	generateCmd.Flags().String("package", defaultPackage, "package name")
	viper.BindPFlag("package", generateCmd.Flags().Lookup("package"))

	generateCmd.Flags().StringP("table", "t", "", "table name")
	viper.BindPFlag("table", generateCmd.Flags().Lookup("table"))

	generateCmd.Flags().String("schema", "", "schema name")
	viper.BindPFlag("schema", generateCmd.Flags().Lookup("schema"))

	generateCmd.Flags().String("struct", "", "struct name")
	viper.BindPFlag("struct", generateCmd.Flags().Lookup("struct"))

	generateCmd.Flags().String("receiver", "r", "receiver name")
	viper.BindPFlag("receiver", generateCmd.Flags().Lookup("receiver"))
}
