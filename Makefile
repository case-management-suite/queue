intTest:
	go test -timeout 30s ./... -integration true -v

test:
	go test -timeout 30s ./... -v