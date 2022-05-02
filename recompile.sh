
echo "start recompile";

cd app\\src\\main\\java\\drz\\oddb\\parse

echo "compile oddb-parse"

javacc parse.jj

echo "compile oddb-parse done"

javac *.java

echo "javac *.java done"
