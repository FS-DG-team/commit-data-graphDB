import matplotlib.pyplot as plt
import json


def linePlot(json_data, test_name):

    # Estrai i dati per ciascun database
    databases = ["neo4j", "tigergraph", "arangodb"]
    data = {db: [json_data[test_name][gb][db]
                 for gb in json_data[test_name]] for db in databases}

    # Colori per i database
    colors = {'neo4j': 'b', 'tigergraph': 'g', 'arangodb': 'r'}

    # Crea il line plot
    plt.figure(figsize=(10, 6))

    for db in databases:
        plt.plot(["1.68GB", "4.85GB", "25.9GB"], data[db], marker='o',
                 linestyle='-', color=colors[db], label=db)

    plt.title('Load Time per Dimensioni dei Dati')
    plt.xlabel('Dimensioni dei Dati (GB)')
    plt.ylabel('Tempo di Caricamento (s)')
    plt.legend()
    plt.grid(True)

    plt.show()


test_names = ["load_time", "processing_and_writing_time", "scenario1",
              "scenario2", "scenario3", "scenario4", "scenario5"]


linePlot(json.load(open('tempi.json', 'r')), test_names[0])
