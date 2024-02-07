import matplotlib.pyplot as plt

# Path to your throughput data file
file_path = 'logs/graph-data.csv'

# Read the data from the file
with open(file_path, 'r') as file:
    # Convert each line to an integer and store in a list
    throughput_data = [int(line.strip()) for line in file]

# Generate a sequence of seconds to use as x-axis (starting from 1)
seconds = list(range(1, len(throughput_data) + 1))

# Plotting the throughput data
plt.figure(figsize=(10, 6))  # Set the figure size for better readability
plt.plot(seconds, throughput_data, marker='o', linestyle='-', color='b')
plt.title('Throughput Over Time')
plt.xlabel('Time (seconds)')
plt.ylabel('Throughput')
plt.grid(True)
plt.show()
