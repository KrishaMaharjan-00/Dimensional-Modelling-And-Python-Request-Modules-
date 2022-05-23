import pandas as pd
import matplotlib.pyplot as plt

def average_data():
    var = pd.read_csv("average.csv")
    x = list(var['temperature'])
    y = list(var['pressure'])
    z = list(var['humidity'])
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    ax.scatter(x, y, z)
    ax.set_xlabel('temperature')
    ax.set_ylabel('pressure')
    ax.set_zlabel('humidity')
    ax.set_title('Average Weather Data')
    plt.show()

def sum_data():
    var = pd.read_csv("sum.csv")
    x = list(var['temperature'])
    y = list(var['pressure'])
    z = list(var['humidity'])
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    ax.scatter(x, y, z)
    ax.set_xlabel('temperature')
    ax.set_ylabel('pressure')
    ax.set_zlabel('humidity')
    ax.set_title('Sum Weather Data')
    plt.show()


if __name__ == "__main__":
    average_data()
    sum_data()