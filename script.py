from flask import Flask, render_template
from pyspark.sql import SparkSession

app = Flask(__name__)

class SparkContextManager:
    def __enter__(self):
        self.spark = SparkSession.builder.appName("MrJob").getOrCreate()
        return self.spark

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'spark') and self.spark:
            self.spark.stop()

# Définir la route pour afficher les résultats
@app.route('/')
def display_results():
    try:
        with SparkContextManager() as spark:
            # Chemin vers le fichier u.data
            data_path = 'Salary.csv'

            # Charger les données dans un RDD
            rdd = spark.sparkContext.textFile(data_path)

            # Supprimer l'en-tête du RDD (première ligne)
            header = rdd.first()
            rdd = rdd.filter(lambda line: line != header)

            # Diviser chaque ligne en champs
            rdd = rdd.map(lambda line: line.split(','))

            # Compter le nombre d'individus pour chaque catégorie de 'Genre'
            gender_counts = rdd.map(lambda fields: (fields[1], 1)).reduceByKey(lambda a, b: a + b).collect()

            # Mapper : Créer une paire (Titre de poste, (Salaire, 1))
            salary_pair_rdd = rdd.map(lambda fields: (fields[3], (float(fields[5]), 1)))

            # Réduire : Calculer la somme des salaires et le nombre d'occurrences pour chaque Titre de poste
            total_salary_counts = salary_pair_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

            # Mapper : Calculer le salaire moyen pour chaque Titre de poste
            average_salary_rdd = total_salary_counts.mapValues(lambda value: value[0] / value[1])
            average_salaries = average_salary_rdd.collect()

            # Filtrer les enregistrements où 'Années d'expérience' sont supérieures à 10
            filtered_records = rdd.filter(lambda fields: float(fields[4]) > 10)

            # Trouver les 3 titres de poste les plus courants parmi les seniors
            senior_records = rdd.filter(lambda fields: fields[8] == '1')
            # Mapper : Créer une paire (Titre de poste, 1) pour chaque enregistrement 'Senior'
            senior_job_titles = senior_records.map(lambda fields: (fields[3], 1))

            # Réduire : Calculer le nombre d'occurrences pour chaque Titre de poste
            job_title_counts = senior_job_titles.reduceByKey(lambda a, b: a + b)

            # Trier par le nombre d'occurrences dans l'ordre décroissant
            sorted_job_titles = job_title_counts.sortBy(lambda x: x[1], ascending=False)

            # Prendre les 3 premiers titres de poste les plus courants
            top_3_job_titles = sorted_job_titles.take(3)

            return render_template('index.html', gender_counts=gender_counts, average_salaries=average_salaries, top_3_job_titles=top_3_job_titles, filtered_records=filtered_records.collect())

    except Exception as e:
        # Gérer les exceptions ici
        return f"Une erreur s'est produite : {str(e)}"

if __name__ == '__main__':
    app.run(debug=True)
