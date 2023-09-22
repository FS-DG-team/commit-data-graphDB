import matplotlib.pyplot as plt
import json
import numpy as np


def linePlot(json_data, test_name):
    # Estrai i dati per ciascun database
    databases = ["neo4j", "tigergraph", "arangodb"]
    data = {db: [json_data[test_name][gb][db]
                 for gb in json_data[test_name]] for db in databases}

    # Calcola i massimi per ciascun punto sull'asse x
    min_values = [np.min([data[db][i] for db in databases])
                  for i in range(len(data[databases[0]]))]

    # Colori per i database
    colors = {'neo4j': 'b', 'tigergraph': 'r', 'arangodb': 'g'}

    # Crea il line plot
    plt.figure(figsize=(10, 6))

    for db in databases:
        plt.plot(["1.68GB", "4.85GB", "20.8GB", "25.9GB"], data[db], marker='o',
                 linestyle='-', color=colors[db], label=db)

    plt.title(f'{test_name} per dimensione dei dati')
    plt.xlabel('Volume dei Dati (GB)')
    plt.ylabel('Tempo impiegato (s)')
    plt.legend()

    # Aggiungi le label in grassetto leggermente più in alto rispetto al punto
    for i, max_val in enumerate(min_values):
        plt.text(i, max_val * 1.05, f'{max_val:.2f}', ha='center',
                 va='bottom', fontsize=10, color='black', weight='bold')

    plt.grid(True)
    plt.yscale('log')

    plt.show()


tests = ["Writing time",  "Scenario 1", "Scenario 2",
         "Scenario 3", "Scenario 4", "Scenario 5"]

for test in tests:
    linePlot(json.load(open('tempi.json', 'r')), test)
