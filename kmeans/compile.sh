javac *.java -d build
jar cfm job.jar Manifest.txt -C build/ .

