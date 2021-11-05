import csv
import statistics

with open('client.csv') as stats:
    csv_reader = csv.reader(stats, delimiter=",")
    throughput_list = []
    for row in csv_reader:
        throughput_list.append(float(row[3]))
    throughput_stats = [min(throughput_list), max(throughput_list), statistics.mean(throughput_list)]

with open('throughput.csv', 'w+') as throughput:
    csv_writer = csv.writer(throughput, delimiter=",")
    csv_writer.writerow(throughput_stats)


