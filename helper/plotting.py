import matplotlib.pyplot as plt
plt.rcParams.update({'font.size': 14})


def box_plot(series, title, label, path):
    fig, ax = plt.subplots(figsize=[12.8, 9.6])
    series.plot.box(ax=ax, showfliers=False, labels=[label])
    ax.set_title(title)
    fig.savefig(path + '_boxplot_wo_fliers.png')
    fig, ax = plt.subplots(figsize=[12.8, 9.6])
    series.plot.box(ax=ax)
    ax.set_title(title)
    fig.savefig(path + '_boxplot_w_fliers.png')


def histogram(series, path, bins):
    fig, ax = plt.subplots(figsize=[12.8, 9.6])
    series.hist(ax=ax, bins=bins)
    fig.savefig(path + '.png')


def pie(df, path, y):
    fig, ax = plt.subplots(figsize=[12.8, 9.6])
    df.plot.pie(ax=ax, y=y, autopct='%.2f')
    fig.savefig(path + '.png')
