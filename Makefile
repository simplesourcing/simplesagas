build:
	mvn -f java/pom.xml install

decrypt:	
	openssl aes-256-cbc -K ${encrypted_9365a57c80a0_key} -iv ${encrypted_9365a57c80a0_iv} -in .deploy/keys.tar.enc -out .deploy/keys.tar -d
	tar xvf .deploy/keys.tar -C .deploy

deploy:
	gpg --import .deploy/keys/private-key.gpg
	mvn -f java/pom.xml install versions:set -DnewVersion=${TRAVIS_TAG}
	mvn -f java/pom.xml install clean deploy -DskipTests -P release --settings .deploy/settings.xml

docs:
	mvn install -DskipTests
	mvn javadoc:javadoc -P sitedocs
	git config --global user.email "travis@travis-ci.com"
	git config --global user.name "travis-ci"

	# Enable SSH authentication
	mv .deploy/keys/docs_deploy_key ${HOME}/.ssh/docs_deploy_key
	chmod 600 "${HOME}/.ssh/docs_deploy_key" \
	         && printf "%s\n" \
	              "Host github.com" \
	              "  IdentityFile ${HOME}/.ssh/docs_deploy_key" \
	              "  LogLevel ERROR" >> ~/.ssh/config

	rm -rf simplesourcing.github.io
	git clone --quiet git@github.com:simplesourcing/simplesourcing.github.io.git > /dev/null
	cd simplesourcing.github.io && \
	    rm -rf apidocs && \
	    cp -Rf ../target/site/apidocs apidocs && \
	    git add . && \
	    git commit -m "Lastest javadoc on successful travis deploy ${TRAVIS_BUILD_NUMBER} auto-pushed to simplesourcing.github.io" && \
	    git push -fq > /dev/null

	rm -rf simplesourcing.github.io
