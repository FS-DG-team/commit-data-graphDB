import os

dataset_path = "./dataset"
output_path = "./datasets"
ext = ".csv"
os.makedirs(output_path, exist_ok=True)

for dir in os.listdir(dataset_path):
    if not os.path.isdir(os.path.join(dataset_path, dir)):
        continue
    with open(os.path.join(output_path, dir+ext), "w", encoding="utf-8") as output_file:
        for file in os.listdir(os.path.join(dataset_path, dir)):
            if not file.endswith(ext):
                continue
            print(os.path.join(dataset_path, dir, file))
            with open(os.path.join(dataset_path, dir, file), "r", encoding="utf-8") as input_file:
                for line in input_file:
                    output_file.write(line)
