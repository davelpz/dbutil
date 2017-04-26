package goutil

import "testing"

func TestOpenDatabase(t *testing.T) {
	d, err := OpenDatabase("root:Baxter5537@/unittest")

	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	err = d.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.ExecSQL("CREATE TABLE `nv` (`name` varchar(64) NOT NULL DEFAULT '',`value` varchar(64) NOT NULL DEFAULT '')")
	if err != nil {
		t.Fatal(err)
	}

	err = d.Commit()
	if err != nil {
		t.Fatal(err)
	}

	err = d.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.ExecSQL("DROP TABLE `nv`")
	if err != nil {
		t.Fatal(err)
	}

	err = d.Commit()
	if err != nil {
		t.Fatal(err)
	}

}
