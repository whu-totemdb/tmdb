
echo "start recompile";

cd app\\src\\main\\java\\drz\\tmdb\\parse

echo "compile tmdb-parse"

javacc parse.jj

echo "compile tmdb-parse done"

javac *.java

echo "javac *.java done"
