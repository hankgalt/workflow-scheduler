package test

type testConfig struct {
	dir       string
	bucket    string
	credsPath string
}

func (c testConfig) DataDir() string {
	return c.dir
}

func (c testConfig) Bucket() string {
	return c.bucket
}

func (c testConfig) CredsPath() string {
	return c.credsPath
}
