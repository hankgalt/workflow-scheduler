package test

type TestConfig interface {
	DataDir() string
	Bucket() string
}

func NewTestConfig(dataDir, bucket string) TestConfig {
	return testConfig{
		dir:    dataDir,
		bucket: bucket,
	}
}

type testConfig struct {
	dir    string
	bucket string
}

func (c testConfig) DataDir() string {
	return c.dir
}

func (c testConfig) Bucket() string {
	return c.bucket
}
