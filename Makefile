.SUFFIXES: .java .class
.java.class:
	javac -cp .:lib/* $*.java

CLASSES = \
		MyDedup.java

MAIN = MyDedup

default: classes

classes: $(CLASSES:.java=.class)

upload: classes
	java -cp .:lib/* MyDedup upload ${min} ${avg} ${max} ${mod} ${filename} ${storage}

download: classes
	java -cp .:lib/* MyDedup download ${filename} ${storage}

delete: classes
	java -cp .:lib/* MyDedup delete ${filename} ${storage}

clean:
	$(RM) *.class
