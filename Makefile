build:
	cd java && mvn install
	cd scala && sbt test
