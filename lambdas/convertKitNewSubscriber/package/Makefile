build:
	rm -rf package/*
	pip freeze --local > requirements.txt
	pip install -r requirements.txt --target package
	cd package
	zip -r9 ../function.zip .
	cd ..
	
info:
	echo 'this is the info'
