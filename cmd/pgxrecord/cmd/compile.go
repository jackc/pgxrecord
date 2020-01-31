package cmd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/jackc/pgxrecord/gen"
	"github.com/spf13/cobra"
)

// compileCmd represents the compile command
var compileCmd = &cobra.Command{
	Use:   "compile RECORD_DESCRIPTION_FILE",
	Args:  cobra.ExactArgs(1),
	Short: "Compile a pgxrecord description into Go",
	Run: func(cmd *cobra.Command, args []string) {
		fileName := args[0]
		fileBytes, err := ioutil.ReadFile(fileName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to read record description file: %v\n", err)
			os.Exit(1)
		}

		table := &gen.Table{}

		err = json.Unmarshal(fileBytes, table)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to parse record description file: %v\n", err)
			os.Exit(1)
		}

		table.Generate(os.Stdout)
	},
}

func init() {
	rootCmd.AddCommand(compileCmd)
}
