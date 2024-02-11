import matplotlib.pyplot as plt
import seaborn as sns

class Plot:
    def __init__(self, main_df):
        self.main_df = main_df
        
    def boxplot(self):
        plt.rcParams['figure.dpi'] = 100
        plt.rcParams['savefig.format'] = 'svg'
        
        fig, ax = plt.subplots(1, 3, figsize=(13, 4))
        plot_data = (['membership_payment_total', 'Total membership payment($)', ax[0]],
                     ['additional_service_total', 'Total additional service($)', ax[1]],
                     ['charged_total', 'Total charge amount($)', ax[2]]
                    )
        
        for i, (y, ylabel, ax) in enumerate(plot_data):
            sns.boxplot(x=self.main_df['membership_plan'], y=self.main_df[y], ax=ax, hue=self.main_df['membership_plan'], palette='Set1')
            ax.set_xlabel('')
            ax.set_ylabel(ylabel, fontsize=11)
            ax.set_ylim(top=self.main_df[y].quantile(.985))
            
        plt.tight_layout()
        
        plt.savefig('img/boxplot.svg')
