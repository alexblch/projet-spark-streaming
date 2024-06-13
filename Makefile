# Cible par défaut
all: package run

# Utilise sbt pour empaqueter le projet
package:
	sbt package

# Exécution de l'application Spark
run:
	spark-submit --class ESGI.App target/scala-2.12/sparkstreamingapp_2.12-0.1.0-SNAPSHOT.jar

# Nettoyage des fichiers générés par sbt
clean:
	sbt clean
