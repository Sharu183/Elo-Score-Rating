import copy
import re
import os
import sys
import string
import glob
import ast
import pandas as pd
import matplotlib.pyplot as plt
import pandas as pd
from collections import Counter
from collections import defaultdict
import warnings

import git
git_repo_object = git.Repo('.', search_parent_directories=True)
git_repo_directory = git_repo_object.working_tree_dir
sys.path.append(os.path.join(git_repo_directory, "src"))

from elorating import calculation
from elorating import dataframe


plt.rcParams["figure.figsize"] = (18,10)

class TubeTestDataPreprocessor:
    def __init__(self,raw_data_file_path,cageList,protocol_name, prefix_name):
        #Member Variables Initialization
        self.raw_data_file_path = raw_data_file_path
        self.inputted_sheet_names_list = cageList
        self.sheet_name_to_everything = defaultdict(dict)
        self.all_winner_id_column = self.winner_id_column = ""
        self.all_loser_id_column = self.loser_id_column = ""
        self.protocol_name = protocol_name
        self.prefix_name = prefix_name
        self.cohort_name = ""
        self.all_sheet_elo_scord_dataframe_list = []
        self.all_sheet_elo_scord_dataframe_combined = pd.DataFrame()
        self.cage_to_strain = {}
        self.index_to_subject_id_and_processed_cage_number = defaultdict(dict)
        self.earliest_date = ""
        self.latest_date = ""
        self.all_cages_string = ""
        self.id_to_final_elo_rating_df = pd.DataFrame()

    def data_preprocessing(self, header_row):
       # Getting the sheet names for the excel file
        xls = pd.ExcelFile(raw_data_file_path)
        raw_data_sheet_names = xls.sheet_names
       
        # %%
        if not self.inputted_sheet_names_list:
            # Making a list out of the string of inputted sheet names
            if inputted_sheet_names_string:
                inputted_sheet_names_string = "[" + inputted_sheet_names_string + "]"
                # Turning the string into a list
                self.inputted_sheet_names_list = ast.literal_eval(inputted_sheet_names_string)
            # Using all the sheet names if no sheet name is specified
            else:
                self.inputted_sheet_names_list =  raw_data_sheet_names

        # %%
        if self.inputted_sheet_names_list:
            for index, sheet in enumerate(self.inputted_sheet_names_list):
                # Checking if the sheet name was a number
                if isinstance(sheet, int) and str(sheet).isdigit():
                    self.inputted_sheet_names_list[index] =  raw_data_sheet_names[sheet]

        # %%
        self.inputted_sheet_names_list

        # %%
        if not set(self.inputted_sheet_names_list).issubset(raw_data_sheet_names):
            # Getting all the sheets that were not in the original spreadsheet
            not_included_sheet_names = set(self.inputted_sheet_names_list) - set(raw_data_sheet_names)
            raise ValueError("All the listed sheet names are not in {}".format(not_included_sheet_names))

        # %%
        # Making the row number a string, so that "0" is treated as a True value
        all_header_row = ""
        if not all_header_row:
            all_header_row = False
        else:
            all_header_row = int(all_header_row)


        # %%
        for sheet in self.inputted_sheet_names_list:
            if all_header_row is False:

                print("\nCurrent Sheet Name: {}".format(sheet))    
                per_sheet_dataframe = pd.read_excel(raw_data_file_path, sheet_name=sheet, header=0)

                # Showing the columns that are chosen with the header being the 0th row
                print("Columns Names: {}".format(per_sheet_dataframe.columns))
                # Show the dataframe that would be created with the header being the 0th row
                print("First few rows of this dataframe:")
                print(pd.read_excel(raw_data_file_path, sheet_name=sheet, header=0).head())

                # Allowing the user the choose the row number for the header

                # header_row = input("""Type the row number to be used as the header
                # (AKA the row with the column name that you want to use.)
                # If you want to keep the column names that were displayed, type 0.
                # If you want to use a different row, then type the corresponding number. 

                # The rows displayed in this cell are dataframes created from Pandas. 
                # To use the row with the 0 index for column names, type 1. 
                # For the row with the 1 index, it will be 2 and so on. i.e. 2

                # If you are looking at the original spread sheet, remember that Python is zero indexed. 
                # So the first row will be 0, second will be 1, and so on. 
                # i.e. 1

                # NOTE: If left blank, the original row that was used will be used.
                # """).strip()

                if header_row == "":
                    header_row = 0
                else:
                    header_row = int(header_row)
            
            # Checking if any of the column names are from empty cells
            column_names = "".join([str(col) for col in pd.read_excel(raw_data_file_path, sheet_name=sheet, header=header_row).columns])
            # If a column name came from an empty cell, it would have "Unnamed" in it
            if "Unnamed" in column_names:
                raise ValueError("""Not all the cells in the chosen row are filled in.
                                Please choose a row that has the name of the columns
                                
                                The values in this row are: {}""".format(column_names))
            else:
                self.sheet_name_to_everything[sheet]["header_row"] = header_row

        # %%
        print(self.sheet_name_to_everything)

    def create_dataframes(self, winner, loser):
        # %%
        # Going through each sheet and creating a dataframe of it
        for key, value in self.sheet_name_to_everything.items():
            value["original_behavior_recording_dataframe"] = pd.read_excel(raw_data_file_path, sheet_name=key, header=value["header_row"])

        # %%
        print(value["original_behavior_recording_dataframe"].head())

        # %%
        print(value["original_behavior_recording_dataframe"].tail())

        # %%
        for key, value in self.sheet_name_to_everything.items():
            # Creating a dictionary that maps the original column name to the standarized one
            column_name_to_standarized = defaultdict(dict)
            for col in value["original_behavior_recording_dataframe"]:
                # Making the column name lower case and removing the spaces
                column_name_to_standarized[col] = "_".join(str(col).lower().strip().split(" "))
            value["column_name_to_standarized"] = column_name_to_standarized

        # %%
        print(value["column_name_to_standarized"])

        # %%
        # Renaming all the columns to the lower case and space removed version
        for key, value in self.sheet_name_to_everything.items():
            value["processed_behavior_recording_dataframe"] = value["original_behavior_recording_dataframe"].rename(columns=value["column_name_to_standarized"])
            value["processed_behavior_recording_dataframe"]["sheet_name"] = key

        # %%
        print(value["processed_behavior_recording_dataframe"].head())

        # %%
        for key, value in self.sheet_name_to_everything.items():
            if not  self.all_winner_id_column:
                # Asking users to specify which column is the one with the winner's information
                value["winner_id_column"] = winner
                # input("""Type the name of the column of the subject that has won the interaction.
                # i.e. "winner"

                # The available columns are: {}

                # Note: If left blank, the column with "winner" in the name will be used as the column
                # """.format(value["processed_behavior_recording_dataframe"].columns))
            
            
                # Looks for columns with "winner" in them if none of inputted
                if not value["winner_id_column"]:
                    value["winner_id_column"] = [col for col in value["processed_behavior_recording_dataframe"].columns if "winner" in col.lower()][0]
            else:
                value["winner_id_column"] =  self.all_winner_id_column
            # Standarizing the column name of the winner IDs
            value["processed_behavior_recording_dataframe"] = value["processed_behavior_recording_dataframe"].rename(columns={value["winner_id_column"]: "winner"})
            value["winner_id_column"] = "winner"
            
            if not  self.all_loser_id_column:   
                # Asking users to specify which column is the one with the winner's information
                value["loser_id_column"] = loser
                
                # input("""Type the name of the column of the subject that has won the interaction.
                # i.e. "loser"

                # The available columns are: {}

                # Note: If left blank, the column with "loser" in the name will be used as the column
                # """.format(value["processed_behavior_recording_dataframe"].columns))

                # Looks for columns with "loser" in them if none of inputted
                if not value["loser_id_column"]:
                    value["loser_id_column"] = [col for col in value["processed_behavior_recording_dataframe"].columns if "loser" in col.lower()][0]
            else:
                value["loser_id_column"] =  self.all_loser_id_column

            # Standarizing the column name of the loser IDs
            value["processed_behavior_recording_dataframe"] = value["processed_behavior_recording_dataframe"].rename(columns={value["loser_id_column"]: "loser"})
            value["loser_id_column"] = "loser"
                
            # Dropping all rows that don't have any values in the winner column
            value["processed_behavior_recording_dataframe"] = value["processed_behavior_recording_dataframe"].dropna(subset=value["winner_id_column"])
            # Dropping all rows that don't have any values in the loser column
            value["processed_behavior_recording_dataframe"] = value["processed_behavior_recording_dataframe"].dropna(subset=value["loser_id_column"])
            
            # Getting all the floats from the strings, removing any spaces and other characters
            value["processed_behavior_recording_dataframe"][value["winner_id_column"]] = value["processed_behavior_recording_dataframe"][value["winner_id_column"]].astype(str).apply(lambda x: re.findall(r"[-+]?(?:\d*\.\d+|\d+)", x)[0] if re.findall(r"[-+]?(?:\d*\.\d+|\d+)", x) else x)
            value["processed_behavior_recording_dataframe"][value["loser_id_column"]] = value["processed_behavior_recording_dataframe"][value["loser_id_column"]].astype(str).apply(lambda x: re.findall(r"[-+]?(?:\d*\.\d+|\d+)", x)[0] if re.findall(r"[-+]?(?:\d*\.\d+|\d+)", x) else x)

        # %%
        print(value["processed_behavior_recording_dataframe"].head())

        # %%
        print(value["processed_behavior_recording_dataframe"].tail())

    def data_cleaning(self,ties_column,session_divider,winner_col_cage_num,loser_col_cage_num):
        
        # %%
        """
        # Finding the rows with ties
        """

        # %%
        """
        - If a trial is a tie, there should be another column that indicates that it is a tie. The points will be counted for less. If there is no column, then none of the trials will be counted as ties.
        """

        # %%
        """
        # NOTE: If there is a set column that denotes whether the match has a winner or not, then replace the `None` with the name of the column with quotation marks
        """

        # %%
        all_ties_column = None

        # %%
        for key, value in self.sheet_name_to_everything.items():
            if all_ties_column is None:
                value["ties_column"] = None
            elif all_ties_column:
                value["ties_column"] = all_ties_column
            else:
            
                # Asking users to specify which column is the one with the winner's information
                value["ties_column"] = ties_column
                # input("""Type the name of the column that specifies whether or not a tie has occured
                # i.e. "tie"

                # The available columns are: {}

                # Note: If left blank, the column with "ties" in the name will be used as the column. 
                # If there are none, then this part will be skipped

                # """.format(value["processed_behavior_recording_dataframe"].columns))

                # Looks for columns with "tie" in them if none of inputted
                if not value["ties_column"]:
                    if [col for col in value["processed_behavior_recording_dataframe"].columns if "tie" in col.lower()]:
                        value["ties_column"] = [col for col in value["processed_behavior_recording_dataframe"].columns if "tie" in col.lower()][0]
                    else: 
                        value["ties_column"] = None
            current_processed_behavior_recording_dataframe = value["processed_behavior_recording_dataframe"].copy()
            try:
                # Standarizing the name of the tie column
                value["processed_behavior_recording_dataframe"] = value["processed_behavior_recording_dataframe"].rename(columns={value["ties_column"]: "match_is_tie"})
                value["ties_column"] = "match_is_tie"
                # Fillling in the tie column with 0s so that we can fill in the other columns with the previous values
                value["processed_behavior_recording_dataframe"][value["ties_column"]] = value["processed_behavior_recording_dataframe"][value["ties_column"]].fillna()
                # Converting all the tie values into bool so we can filter for the cells with ties
                value["processed_behavior_recording_dataframe"][value["ties_column"]] = value["processed_behavior_recording_dataframe"][value["ties_column"]].astype(bool)
            except:
                value["processed_behavior_recording_dataframe"] = current_processed_behavior_recording_dataframe
                value["ties_column"] = None

        # %%
        """
        ## Adding the session number
        """

        # %%
        """
        - We are adding the session number to all the trials. The session number is counting the number of recording sessions that have happened up until that trial. Usually, each session in the spreadsheet is divided up by a session's first row having the date filled in. So we will label a new session when a date is filled in.
        """

        # %%
        """
        # NOTE: If there is a set column that divides the rows up by session, then enter it in the cell below between the quotation marks. Default is `"date"`
        """

        # %%
        """
        # TODO: Recheck starting here
        """

        # %%
        all_session_divider_column = ""

        # %%
        for key, value in self.sheet_name_to_everything.items():
            if not all_session_divider_column:
                # Allowing the user to choose the column that indicates a new session
                session_divider_column = session_divider
                
                # input("""Type the name of the column to be used to divide the rows into sessions. 
                # Pick a column that has a value in the cell only with the first row of each session.
                # This is usually the "date" column.

                # If left blank, the default session divider column will be used. The default is "date"

                # The available columns are: {}

                # NOTE: If left blank, the column with "date" in the name will be used as the column
                # """.format(value["processed_behavior_recording_dataframe"].columns))
            else:
                session_divider_column = all_session_divider_column
                
            # Making the column name lowercase and removing any punctuation
            value["session_divider_column"] = session_divider_column.lower().strip('"').strip("'").strip()

            # Looks for columns with "date" in them if none is inputted
            if not value["session_divider_column"]:
                value["session_divider_column"] = [col for col in value["processed_behavior_recording_dataframe"].columns if "date" in col.lower()][0]

            # Checks if that column is in the dataframe
            if value["session_divider_column"] not in value["processed_behavior_recording_dataframe"].columns:
                print("WARNING: {} is not a column in {}".format(value["session_divider_column"], key))
                warnings.warn("Look at warning from above or below")
                value["session_divider_column"] = None
            
            # Standarizing all the session divider columns so that they are named date
            if value["session_divider_column"]:
                value["processed_behavior_recording_dataframe"] = value["processed_behavior_recording_dataframe"].rename(columns={value["session_divider_column"]: "date"})
                value["session_divider_column"] = "date"

        # %%


        # %%
        """
        # TODO: Make this standarized between sheets and get difference for each row
        """

        # %%
        for key, value in self.sheet_name_to_everything.items():
            current_processed_behavior_recording_dataframe = value["processed_behavior_recording_dataframe"].copy()
            try:
                value["processed_behavior_recording_dataframe"][value["session_divider_column"]] = value["processed_behavior_recording_dataframe"][value["session_divider_column"]].fillna(method='ffill')

            except:
                print("WARNING: {} does not have a session divider column".format(key))
                warnings.warn("Look at warning from above or below")
                value["processed_behavior_recording_dataframe"] = current_processed_behavior_recording_dataframe


        # %%
        value["processed_behavior_recording_dataframe"].head()

        # %%
        value["processed_behavior_recording_dataframe"].tail()

        # %%
        """
        # Getting the Session number differences
        """

        # %%
        """
        - Getting the indexes of where each new session starts. So that we can add the session number to each row.
        """

        # %%
        """
        # TODO: Can remove set about not including tie columns
        """

        # %%
        # Example of the columns that will be kept, removing the ties column
        list(set(value["processed_behavior_recording_dataframe"].columns) - set([value["ties_column"]]))

        # %%
        for key, value in self.sheet_name_to_everything.items():
            # Removing tie columns because not all rows are ties, so we do not want to fill them in
            if value["ties_column"]:
                non_ties_columns = list(set(value["processed_behavior_recording_dataframe"].columns) - set([value["ties_column"]]))
                value["processed_behavior_recording_dataframe"][non_ties_columns] = value["processed_behavior_recording_dataframe"][non_ties_columns].fillna(method='ffill')
            else:
                # Filling all the empty cells with the value in the previous cell
                value["processed_behavior_recording_dataframe"] = value["processed_behavior_recording_dataframe"].fillna(method='ffill')

            try:
                # Seeing which rows have a different session than the previous one
                # This will be used to plot vertical lines for each new session
                value["processed_behavior_recording_dataframe"]["session_number_difference"] = value["processed_behavior_recording_dataframe"][value["session_divider_column"]].astype('category').cat.codes.diff()

            
            except:
                print("WARNING: {} does not have a session divider column".format(key))
                warnings.warn("Look at warning from above or below")

        # %%
        value["processed_behavior_recording_dataframe"].head(n=15)

        # %%
        value["processed_behavior_recording_dataframe"].head(n=15)

        # %%
        value["processed_behavior_recording_dataframe"].tail(n=15)

        # %%
        """
        ## Getting the cage number
        """

        # %%
        """
        - The cage number is sometimes not consistent throughout a spreadsheet for the same cage. So we will try to standarize it into one value.
            - **NOTE**: This assumes cage numbers are actual numbers. And not entirely consisting of letters. If that isn't the case, then you must edit this cell for your needs.
        """

        # %%
        """
        # NOTE: If there is a column that has the cage number information, replace the `None` with the column name in quotation marks
        """

        # %%
        """
        # TODO: Refactor starting here
        """

        # %%
        """
        - Change this when you have winner and loser cage
        """

        # %%
        cage_num_of_winner_column = None
        cage_num_of_loser_column = None

        # %%
        """
        - Change this when you have id to cage
        """

        # %%
        id_to_cage = {}

        # %%
        for key, value in self.sheet_name_to_everything.items():   
            # When you have a dictionary of IDs to Cage Numbers
            if id_to_cage:
                # Specifying the name by default
                value["cage_num_of_winner_column"] = "original_cage_num_of_winner"
                value["cage_num_of_loser_column"] = "original_cage_num_of_loser"
                # Mapping the IDs to the cage number
                value["processed_behavior_recording_dataframe"][value["cage_num_of_winner_column"]] = value["processed_behavior_recording_dataframe"][value["winner_id_column"]].map(id_to_cage)
                value["processed_behavior_recording_dataframe"][value["cage_num_of_loser_column"]] = value["processed_behavior_recording_dataframe"][value["loser_id_column"]].map(id_to_cage)
                continue
                
            # When you have a column that specifies the cage number
            elif cage_num_of_winner_column and cage_num_of_loser_column:
                value["cage_num_of_winner_column"] = cage_num_of_winner_column
                value["cage_num_of_loser_column"] = cage_num_of_loser_column
            elif cage_num_of_winner_column:
                value["cage_num_of_winner_column"] = cage_num_of_winner_column
            elif cage_num_of_loser_column:
                value["cage_num_of_loser_column"] = cage_num_of_loser_column

            
            if not cage_num_of_winner_column:
                # Asking the user which column has the cage number
                value["cage_num_of_winner_column"] = winner_col_cage_num
                # input("""Type the name of the column of the cage of the WINNER subject
                # i.e. "cage_num_of_winner"

                # The available columns are: {}

                # Note: If left blank, the column with "winner" and "cage" will be used as the column. If there is none, then the sheet name will be used.
                # """.format(value["processed_behavior_recording_dataframe"].columns))        
                
                # Getting the column that has "winner" and "cage" in its name if no column is specified
                if not value["cage_num_of_winner_column"]:
                    # Checking to see if there are any columns with the winner and cage in the name
                    column_names_with_cage = [col for col in value["processed_behavior_recording_dataframe"].columns if "cage" in col.lower() and "winner" in col.lower()]
                    # Using the column with cage in the name if there are none with winner and cage
                    column_names_with_cage += [col for col in value["processed_behavior_recording_dataframe"].columns if "cage" in col.lower()]
                    if column_names_with_cage:
                        value["winnner_cage_column"] = column_names_with_cage[0]
                    else:
                        # Using the default name as the cage column name if there are none
                        value["cage_num_of_winner_column"] = "original_cage_num_of_winner"
            
            if not cage_num_of_loser_column:
                # Asking the user which column has the cage number
                value["cage_num_of_loser_column"] = loser_col_cage_num
                # input("""Type the name of the column of the cage of the LOSER subject
                # i.e. "cage_num_of_loser"

                # The available columns are: {}

                # Note: If left blank, the column with "loser" and "cage" will be used as the column. If there is none, then the sheet name will be used.
                # """.format(value["processed_behavior_recording_dataframe"].columns))        
                
                # Getting the column that has "loser" and "cage" in its name if no column is specified
                if not value["cage_num_of_loser_column"]:
                    # Checking to see if there are any columns with the loser and cage in the name
                    column_names_with_cage = [col for col in value["processed_behavior_recording_dataframe"].columns if "cage" in col.lower() and "loser" in col.lower()]
                    # Using the column with cage in the name if there are none with loser and cage
                    column_names_with_cage += [col for col in value["processed_behavior_recording_dataframe"].columns if "cage" in col.lower()]
                    if column_names_with_cage:
                        value["winnner_cage_column"] = column_names_with_cage[0]
                    else:
                        # Using the default name as the cage column name if there are none
                        value["cage_num_of_loser_column"] = "original_cage_num_of_loser"
            
            # Using the spreadsheet name as the cage number if there are no columns that match
            if value["cage_num_of_winner_column"] not in value["processed_behavior_recording_dataframe"].columns:
                value["processed_behavior_recording_dataframe"][value["cage_num_of_winner_column"]] = key

            if value["cage_num_of_loser_column"] not in value["processed_behavior_recording_dataframe"].columns:
                value["processed_behavior_recording_dataframe"][value["cage_num_of_loser_column"]] = key       
            
            # Turning the cage names into the float number only
            # Or using the same name if there are no floats
            value["processed_behavior_recording_dataframe"]["processed_cage_num_of_winner"] = value["processed_behavior_recording_dataframe"][value["cage_num_of_winner_column"]].apply(lambda x: re.findall(r'\d+', str(x))[0] if re.findall(r'\d+', str(x)) else x)
            value["processed_behavior_recording_dataframe"]["processed_cage_num_of_loser"] = value["processed_behavior_recording_dataframe"][value["cage_num_of_loser_column"]].apply(lambda x: re.findall(r'\d+', str(x))[0] if re.findall(r'\d+', str(x)) else x)

        # %%
        print(self.sheet_name_to_everything[key]["processed_behavior_recording_dataframe"])
    
    def calculate_process_elorating(self,cols_to_keep):


        # %%
        """
        ## Calculating Elo rating
        """

        # %%
        """
        - Example calculation
        """

        # %%
        calculation.calculate_elo_rating(subject_elo_rating=1000, agent_elo_rating=2000, score=1)

        # %%
        calculation.update_elo_rating(winner_id="A", loser_id="B")

        # %%
        """
        ## Get the Elo rating for all the events
        """

        # %%
        """
        - Going through each row or interaction and calculating the new Elo rating for the winner and loser. This will create a new dataframe based off of the processed behavioral recording dataframe
        """

        # %%
        """
        # NOTE: If there are a set of columns to keep, edit the cell below with the name of the columns each in quotation marks seperated by commas
        - i.e. `['runner', 'date', 'match', 'winner', 'loser', 'notes', 'session_number',
            'session_number_difference']`
        """

        # %%
        all_columns_to_keep = []

    # %%
        def get_subject_cage_number(x,cols_to_keep):
            """
            """
            if x["win_draw_loss"] == 1:
                return x["processed_cage_num_of_winner"]
            elif x["win_draw_loss"] == 0:
                return x["processed_cage_num_of_loser"]
            elif x["win_draw_loss"] == 0.5:
                if 1 == self.pairing_index:
                    return x["processed_cage_num_of_winner"] 
                elif 1 == self.pairing_index:
                    return x["processed_cage_num_of_loser"]

        # %%
        copy_sheet_name_to_everything = copy.copy(self.sheet_name_to_everything)
        for key, value in self.sheet_name_to_everything.items():
            try:
                if not all_columns_to_keep:
                    # Asking the user which columns to carry over to the Elo rating calculation dataframe
                    value["columns_to_keep_string"] = cols_to_keep
                    
                    # input("""Type all the columns that will be copied from the original dataframe to the Elo rating calculated dataframe. 

                    # All the available actions: {}
                    # Each column must be put in quotes and seperated by a comma(,). i.e. 'length of observations', 'date', 'cage #'

                    # NOTE: If left blank, all the columns will be kept
                    # """.format(value["processed_behavior_recording_dataframe"].columns))


                    # Making a list out of the string of inputted sheet names
                    if value["columns_to_keep_string"]:
                        value["columns_to_keep_string"] = "[" + value["columns_to_keep_string"] + "]"
                        value["columns_to_keep_list"] = ast.literal_eval(value["columns_to_keep_string"])
                    else:
                        value["columns_to_keep_list"] = value["processed_behavior_recording_dataframe"].columns
                else: 
                    value["columns_to_keep_list"] = all_columns_to_keep

                try:
                    # Calculating the Elo rating    
                    value["index_to_elo_rating_and_meta_data"] = calculation.iterate_elo_rating_calculation_for_dataframe(dataframe=value["processed_behavior_recording_dataframe"], \
                                                                                                                            winner_id_column=value["winner_id_column"], loser_id_column=value["loser_id_column"], \
                                                                                                                                        additional_columns=value["columns_to_keep_list"], tie_column=value["ties_column"])

                except:
                    nonexistent_columns = set(value["columns_to_keep_list"]) - set(value["processed_behavior_recording_dataframe"].columns)
                    print("WARNING: {} does not have {} columns".format(key, nonexistent_columns))
                    warnings.warn("Look at warning from above or below")
                    # Using all the column names if no column name is specified
                    # Removing the winner and loser column because they will be specified as the columns for the Elo rating calculation
                    value["columns_to_keep_list"] =  sorted(list(set(value["processed_behavior_recording_dataframe"].columns) - set([value["winner_id_column"]]) - set([value["loser_id_column"]])))
                    value["index_to_elo_rating_and_meta_data"] = calculation.iterate_elo_rating_calculation_for_dataframe(dataframe=value["processed_behavior_recording_dataframe"], \
                                                                                                                            winner_id_column=value["winner_id_column"], loser_id_column=value["loser_id_column"], \
                                                                                                                                        additional_columns=value["columns_to_keep_list"], tie_column=value["ties_column"])     

                # Making a dataframe from the dictionary 
                value["elo_rating_dataframe"] = pd.DataFrame.from_dict(value["index_to_elo_rating_and_meta_data"], orient="index")
                try:
                    value["elo_rating_dataframe"]["cage_num_of_subject"] = value["elo_rating_dataframe"].apply(lambda x: get_subject_cage_number(x), axis = 1)
                    value["elo_rating_dataframe"]["cage_num_of_agent"] = value["elo_rating_dataframe"].apply(lambda x: get_subject_cage_number(x), axis = 1)
                except:
                    value["elo_rating_dataframe"]["cage_num_of_subject"] = key
                    value["elo_rating_dataframe"]["cage_num_of_agent"] = key
            except:
                copy_sheet_name_to_everything.pop(key)
        sheet_name_to_everything = copy_sheet_name_to_everything

        # %%

        print(value["elo_rating_dataframe"])

        # %%
        print(value["elo_rating_dataframe"].head())

        # %%
        print(value["elo_rating_dataframe"].tail())

        # %%
        print(value["elo_rating_dataframe"].groupby("subject_id").count())

        print("---------------------------------------------------------------------------------------------------------------------------------------------")

        print(len(sheet_name_to_everything.items()))

    def merge_process_eloratings(self):

        # %%
        """
        ## Combining all the Elo rating dataframes into one
        """

        # %%
        # Putting all the dataframes into one list
        for key, value in self.sheet_name_to_everything.items():    
            self.all_sheet_elo_scord_dataframe_list.append(value["elo_rating_dataframe"])

        # Combining all the dataframes into one
        self.all_sheet_elo_scord_dataframe_combined = pd.concat(self.all_sheet_elo_scord_dataframe_list)

        # %%
        self.all_sheet_elo_scord_dataframe_combined

        # %%
        """
        - Adding the strain information
        """

        # %%
        all_subject_ids = set(self.all_sheet_elo_scord_dataframe_combined["subject_id"].unique()).union(set(self.all_sheet_elo_scord_dataframe_combined["agent_id"].unique()))

        # %%
        """
        # NOTE: If there are strains that are associated to each cage, then create a dictionary of cage numbers to strains inside the `{}`
        - i.e. `cage_to_strain = {"1": "C57", "2": "C57", "3": "C57", "4": "CD1", "5": "CD1", "6": "CD1"}`
        """

        # %%
        

        # %%
        if self.cage_to_strain:
            self.all_sheet_elo_scord_dataframe_combined["subject_strain"] = self.all_sheet_elo_scord_dataframe_combined["cage_num_of_subject"].map(self.cage_to_strain)
            self.all_sheet_elo_scord_dataframe_combined["agent_strain"] = self.all_sheet_elo_scord_dataframe_combined["cage_num_of_agent"].map(self.cage_to_strain)

        # %%
        """
        - Adding the name of the experiment
        """

        # %%
        # Adding the name of the experiment
        self.all_sheet_elo_scord_dataframe_combined["experiment_type"] = protocol_name

        # %%
        """
        - Adding the cohort
        """

        # %%
        self.all_sheet_elo_scord_dataframe_combined["cohort"] = self.cohort_name

        # %%
        self.all_sheet_elo_scord_dataframe_combined.head()

        # %%
        self.all_sheet_elo_scord_dataframe_combined.tail()

        # %%
        # Checking to see how many rows for each subject in each cage
        print(self.all_sheet_elo_scord_dataframe_combined.groupby(['subject_id','cage_num_of_subject']).count())

    def create_final_eloratings(self):

        # %%
        # Checking to see which cage and subject combination has more than one row
        all_sheet_elo_scord_dataframe_groupby = self.all_sheet_elo_scord_dataframe_combined.groupby(['subject_id','cage_num_of_subject']).size().reset_index()
        all_sheet_elo_scord_dataframe_groupby = all_sheet_elo_scord_dataframe_groupby[all_sheet_elo_scord_dataframe_groupby[0] >= 1]

        # Going through each combination and saving the combination to a dictionary
        
        for index, row in all_sheet_elo_scord_dataframe_groupby.iterrows():
            self.index_to_subject_id_and_processed_cage_number[index]['subject_id'] = row['subject_id']
            self.index_to_subject_id_and_processed_cage_number[index]['cage_num_of_subject'] = row['cage_num_of_subject']


        # %%
        print(self.index_to_subject_id_and_processed_cage_number)

        # %%
        for index, (key, value) in enumerate(self.index_to_subject_id_and_processed_cage_number.items()):   
            # The Elo rating dataframe for each cage and subject combination
            per_subject_dataframe = self.all_sheet_elo_scord_dataframe_combined[(self.all_sheet_elo_scord_dataframe_combined["subject_id"] == value["subject_id"]) & (self.all_sheet_elo_scord_dataframe_combined["cage_num_of_subject"] == value["cage_num_of_subject"])]
            # Getting the final Elo rating for each combination
            # -1 Means that we're getting the data from the last row

            self.index_to_subject_id_and_processed_cage_number[index]["final_elo_rating"] = per_subject_dataframe.iloc[-1]["updated_elo_rating"]
            self.index_to_subject_id_and_processed_cage_number[index]["cohort"] = per_subject_dataframe.iloc[-1]["cohort"]
            try:
                self.index_to_subject_id_and_processed_cage_number[index]["strain"] = per_subject_dataframe.iloc[-1]["strain"]
            except:
                print("WARNING: {} in cage {} does not have strain information".format(self.index_to_subject_id_and_processed_cage_number[key]["subject_id"], self.index_to_subject_id_and_processed_cage_number[key]["cage_num_of_subject"]))
                warnings.warn("Look at warning from above or below")

    # %%
        self.id_to_final_elo_rating_df = pd.DataFrame.from_dict(self.index_to_subject_id_and_processed_cage_number, orient="index")
        # Adding protocol name
        self.id_to_final_elo_rating_df["experiment_type"] = protocol_name
        # Adding rank
        self.id_to_final_elo_rating_df["rank"] = self.id_to_final_elo_rating_df.groupby("cage_num_of_subject")["final_elo_rating"].rank("dense", ascending=False)
        # Sorting by cage and then id
        self.id_to_final_elo_rating_df = self.id_to_final_elo_rating_df.sort_values(by=['cage_num_of_subject', "subject_id"], ascending=True).reset_index(drop=True)

        # %%
        print(self.id_to_final_elo_rating_df.head())

        # %%
        print(self.id_to_final_elo_rating_df.tail())



    def make_plots(self):

        # %%
        """
        # Making plots for all sheets
        """

        # %%
        """
        - Getting the dates the files were being recorded to use for the file name
        """

        # %%
        self.all_sheet_elo_scord_dataframe_combined.head()

        # %%
        self.all_sheet_elo_scord_dataframe_combined.tail()

        # %%
        """
        - Getting the earliest and the latest dates for all the recordings
        """

        # %%
        all_earlist_dates = []
        all_latest_dates = []
        for key, value in self.sheet_name_to_everything.items():
            try:
                # Getting all the earliest dates for each sheet
                all_earlist_dates.append(value["elo_rating_dataframe"][value["session_divider_column"]].min())
                all_latest_dates.append(value["elo_rating_dataframe"][value["session_divider_column"]].max())
            except:
                print("WARNING: {} does not have dates as columns".format(key))
                warnings.warn("Look at warning from above or below")

        # %%
        try:
            # Turning the Dates into a easier to read format
            # Getting the 0th part of split to remove seconds
            self.earliest_date = str(min(all_earlist_dates)).split()[0]
            self.latest_date = str(max(all_latest_dates)).split()[0]
            print("Earlist date: {}".format(self.earliest_date))
            print("Latest date: {}".format(self.latest_date))
        except:
            self.earliest_date = None
            self.latest_date = None

        # %%
        """
        - Getting the cage numbers
        """

        # %%
        all_cages_list = []
        # Creating a list of all the cage numbers
        for key, value in self.sheet_name_to_everything.items():
            try:
                for cage in value["elo_rating_dataframe"]["cage_num_of_subject"].unique():
                    all_cages_list.append(cage)
            except:
                print("WARNING: {} does not have cage number as columns".format(key))
                warnings.warn("Look at warning from above or below")

        # %%
        try:
            self.all_cages_string = "-".join(sorted([sheet.lower().strip("cage").strip() for sheet in all_cages_list]))
            self.all_cages_string = "cages-{}".format(self.all_cages_string)
            print("String of cage names to use for file name: {}".format(self.all_cages_string))
        except: 
            warnings.warn("WARNING: There are no cage numbers to make a title out of")
            self.all_cages_string = None

        # %%
        """
        - Creating an output directory to save the plots
        """

        # %%
        plot_output_directory = os.path.join(".", "proc", "plots", "{}_elo_rating".format(protocol_name))

        # %%
        plot_output_directory

        # %%
        os.makedirs(plot_output_directory, exist_ok=True)

        # %%
        """
        # **NOTE**: Sometimes this cell needs to be run again to make sure the size is correct
        """

        # %%
        # Getting the highest and lowest Elo rating for cutoffs of the Y-axis
        max_elo_rating = self.all_sheet_elo_scord_dataframe_combined["updated_elo_rating"].max()
        min_elo_rating = self.all_sheet_elo_scord_dataframe_combined["updated_elo_rating"].min()

        plt.rcParams["figure.figsize"] = (13.5,7.5)
        # Making a plot for each sheet
        for key, value in self.sheet_name_to_everything.items():
            # Setting the size of the figure
            plt.rcParams["figure.figsize"] = (13.5,7.5)
            print(key)
            elo_rating_dataframe = value["elo_rating_dataframe"]
            # Using a new figure template for each sheet
            fig, ax = plt.subplots()        
                
            try:
                # Drawing vertical lines that represent when each session begins
                # Based on when a row has a different session than the previous row
                for index, row in elo_rating_dataframe[elo_rating_dataframe['session_number_difference'].astype(bool)].iterrows():
                    # Offsetting by 0.5 to avoid drawing the line on the dot
                    # Drawing the lines a little above the max and a little below the minimum
                    plt.vlines(x=[row["total_match_number"] - 0.5], ymin=min_elo_rating-50, ymax=max_elo_rating+50, colors='black', linestyle='dashed')
            except:
                print("WARNING: {} does not have a column for session divider".format(key))
                warnings.warn("Look at warning from above or below")
                    
            # Drawing a line for each subject
            for subject in sorted(elo_rating_dataframe["subject_id"].unique()):
                # Getting all the rows with the current subject
                subject_dataframe = elo_rating_dataframe[elo_rating_dataframe["subject_id"] == subject]
                # Making the current match number the X-Axis
                plt.plot(subject_dataframe["total_match_number"], subject_dataframe["updated_elo_rating"], '-o', label=subject)

            # Labeling the X/Y Axis and the title
            ax.set_xlabel("Trial Number")
            ax.set_ylabel("Elo rating")
            # Formattnig Cohort and Experiment Name so that it's more readable with spacing and capitalization
            try:
                formatted_cohort_name = " ".join(self.cohort_name.split("_")).capitalize()
            except:
                formatted_cohort_name = self.cohort_name
            try:
                formatted_protocol_name = string.capwords(" ".join(protocol_name.split("_")))
            except:
                formatted_protocol_name = protocol_name
            try:
                formatted_cage_name = " ".join((re.match(r"([a-z]+)([0-9]+)", key, re.I).groups())).capitalize()    
            except:
                formatted_cage_name = key
            ax.set_title("{} Elo Rating for {} {}".format(formatted_protocol_name, formatted_cohort_name, formatted_cage_name))
            
            # To show the legend
            ax.legend(loc="upper left")
            # Setting the values of the Y-axis
            plt.ylim(min_elo_rating-50, max_elo_rating+50) 
            # Saving the plot
            file_name_parts_separated = [prefix_name, self.cohort_name, key, self.earliest_date, self.latest_date]
            file_name_parts_combined = "_".join([part for part in file_name_parts_separated if part])
            
            file_name_full = "elo_rating_{}.png".format(file_name_parts_combined)
            # Removing all the spaces and replacing them with underscores
            file_name_full = "_".join(file_name_full.split(" "))
            plt.savefig(os.path.join(plot_output_directory, file_name_full))
            # Showing the plots
            plt.show()

    def save_dataframes(self):
        
        # %%
        """
        # Saving the Dataframes
        """

        # %%
        """
        - Creating a subfolder to put the Elo rating Spreadsheets
        """

        # %%
        elo_rating_spreadsheet_output_directory = os.path.join(".", "proc", "elo_rating_spread_sheets", "{}".format(protocol_name))

        # %%
        elo_rating_spreadsheet_output_directory

        # %%
        os.makedirs(elo_rating_spreadsheet_output_directory, exist_ok=True)

        # %%
        """
        - Saving the dataframes to a file
        """

        # %%
        file_name_parts_separated = [self.cohort_name, self.all_cages_string, prefix_name, self.earliest_date, self.latest_date]
        file_name_parts_combined = "_".join([part for part in file_name_parts_separated if part])

        file_name_full = "{}_elo-rating-history.csv".format(file_name_parts_combined)
        print(file_name_full)
        self.all_sheet_elo_scord_dataframe_combined.to_csv(os.path.join(elo_rating_spreadsheet_output_directory, file_name_full))

        # %%
        file_name_full = "{}_final-elo-rating.csv".format(file_name_parts_combined)
        print(file_name_full)
        self.id_to_final_elo_rating_df.to_csv(os.path.join(elo_rating_spreadsheet_output_directory, file_name_full))

        # %%
        """
        # Seeing which subject is the dominant or submissive
        """

        # %%
        """
        - Grouping all the rows with the same pair
        """

        # %%
        all_processed_behavior_recording_list = []
        for key, value in self.sheet_name_to_everything.items():
            all_processed_behavior_recording_list.append(value["processed_behavior_recording_dataframe"])
            

        # %%
        """
        - Combining all the dataframes from all the cages
        """

        # %%
        all_processed_behavior_recording_df = pd.concat(all_processed_behavior_recording_list)

        # %%
        all_processed_behavior_recording_df.head()

        # %%
        """
        - Getting a tuple of the animal IDs to be able to group
        """

        # %%
        """
        # Note: This assumes that all the animal IDs are different across cages and that all IDs are numbers. i.e. there are no duplicate IDs in different cages.
        """

        # %%
        # Getting the animal IDs from the Match string
        # i.e. Getting all the floats and removing all spaces
        # Sorting so that the animal IDs are always in the same order
        all_processed_behavior_recording_df["animal_id"] =  all_processed_behavior_recording_df["match"].apply(lambda x: sorted([subject_id.lower().strip() for subject_id in re.findall(r"[-+]?(?:\d*\.\d+|\d+)", x)]))


        # %%
        # Making a tuple out of the list
        # Tuples are used because lists are mutable and can't be grouped with
        all_processed_behavior_recording_df["tuple_animal_id"] = all_processed_behavior_recording_df["animal_id"].apply(lambda x: tuple(x))

        # %%
        all_processed_behavior_recording_df.head()

        # %%
        """
        - Removing columns that would be unnecessary for the pairings
        """

        # %%
        all_processed_behavior_recording_df.columns

        # %%
        # Getting only the columns that we need
        all_processed_behavior_recording_df = all_processed_behavior_recording_df[['runner', 'date', 'match', 'winner', 'loser', 'notes', 'animal_id', 'tuple_animal_id', "processed_cage_num_of_winner", "processed_cage_num_of_loser"]]

        # %%
        all_processed_behavior_recording_df.head()

        # %%
        """
        - Getting the ID of the winner and the loser for each pair with each match
        """

        # %%
        all_wins_per_pair = all_processed_behavior_recording_df.groupby("tuple_animal_id")['winner'].apply(list)
        all_loses_per_pair = all_processed_behavior_recording_df.groupby("tuple_animal_id")['loser'].apply(list)

        # %%
        all_wins_per_pair[:5]

        # %%
        """
        - Making a dataframe of all the winner IDs and all the loser IDs for a given pair
        """

        # %%
        all_competition_per_pair_df = pd.concat([all_wins_per_pair, all_loses_per_pair], axis=1).reset_index()

        # %%
        all_competition_per_pair_df = all_competition_per_pair_df.rename(columns={k: prefix_name + "_" + k for k in all_competition_per_pair_df.columns})

        # %%
        all_competition_per_pair_df

        # %%
        """
        - Adding the cage information
        """

        # %%
        # Getting the cage number for each pair
        dropped_duplicate_all_processed_behavior_recording_df = all_processed_behavior_recording_df[["tuple_animal_id"]].drop_duplicates()

        # %%
        dropped_duplicate_all_processed_behavior_recording_df.head()

        # %%
        """
        - Creating a dictionary so that we can create a column for the cage number based on the IDs
        """

        # %%
        """
        ## TODO: EDIT below
        """

        # %%
        try:
            pair_to_cage = pd.Series(dropped_duplicate_all_processed_behavior_recording_df["processed_cage_number"].values, index=dropped_duplicate_all_processed_behavior_recording_df["tuple_animal_id"]).to_dict()
            self.display(pair_to_cage)
        except:
            #TODO: 
            pass

        # %%
        try:
            all_competition_per_pair_df["processed_cage_number"] = all_competition_per_pair_df["{}_tuple_animal_id".format(prefix_name)].map(pair_to_cage)
            all_competition_per_pair_df["processed_cage_number"] = all_competition_per_pair_df["processed_cage_number"].astype(int).astype(str)
            self.display(all_competition_per_pair_df.head())
        except:
            pass

        # %%
        all_competition_per_pair_df["cohort"] = self.cohort_name


        # %%
        """
        - Calculating the overall winner and loser. Also seeing if there is signficant difference in the number of wins to see if one is dominant over the other
        """

        # %%
        all_competition_per_pair_df["{}_averaged_winner".format(prefix_name)] = all_competition_per_pair_df["{}_winner".format(prefix_name)].apply(lambda x: Counter(x).most_common(1)[0][0])
        all_competition_per_pair_df["{}_averaged_loser".format(prefix_name)] = all_competition_per_pair_df["{}_loser".format(prefix_name)].apply(lambda x: Counter(x).most_common(1)[0][0])
        all_competition_per_pair_df["{}_winner_count".format(prefix_name)] = all_competition_per_pair_df.apply(lambda x: x["{}_winner".format(prefix_name)].count(x["{}_averaged_winner".format(prefix_name)]), axis=1)
        all_competition_per_pair_df["{}_loser_count".format(prefix_name)] = all_competition_per_pair_df.apply(lambda x: x["{}_winner".format(prefix_name)].count(x["{}_averaged_loser".format(prefix_name)]), axis=1)
        all_competition_per_pair_df["{}_count_difference".format(prefix_name)] = all_competition_per_pair_df["{}_winner_count".format(prefix_name)] - all_competition_per_pair_df["{}_loser_count".format(prefix_name)]
        all_competition_per_pair_df["{}_match_count".format(prefix_name)] = all_competition_per_pair_df["{}_winner".format(prefix_name)].apply(lambda x: len(x))
        all_competition_per_pair_df["{}_percent_win".format(prefix_name)] = all_competition_per_pair_df.apply(lambda x: x["{}_winner_count".format(prefix_name)] / x["{}_match_count".format(prefix_name)], axis=1)
        all_competition_per_pair_df["{}_percentage_tie".format(prefix_name)] = all_competition_per_pair_df["{}_percent_win".format(prefix_name)].apply(lambda x: True if x < 0.75 else False)

        # %%
        all_competition_per_pair_df

        # %%
        """
        - Saving the competiton pair results dataframe to a file
        """

        # %%
        file_name = "{}_{}_grouped_by_pairs_cage_{}_date_{}_{}.csv".format(self.cohort_name, prefix_name, self.all_cages_string, self.earliest_date, self.latest_date)


        # %%
        elo_rating_spreadsheet_output_directory

        # %%
        file_name

        # %%
        all_competition_per_pair_df.to_csv(os.path.join(elo_rating_spreadsheet_output_directory, file_name))

    # header_row, Winner, Loser,
    def main(self,header_row, winner,loser, ties_column, session_divider, winner_col_cage_num, loser_col_cage_num,cols_to_keep):
        
        self.data_preprocessing(header_row)
        self.create_dataframes(winner, loser)
        self.data_cleaning(ties_column,session_divider,winner_col_cage_num, loser_col_cage_num)
        self.calculate_process_elorating(cols_to_keep)
        self.merge_process_eloratings()
        self.create_final_eloratings()
        self.make_plots()
        self.save_dataframes()

if __name__ == '__main__':
    raw_data_file_path = './data/pilot_3_tube_test.xlsx'
    protocol_name = "tube_test"
    prefix_name = "tt"
    cageList = ['CAGE1', 'CAGE2', 'CAGE3', 'CAGE4','CAGE5','CAGE6'];
    args= sys.argv
    header_row= args[1]
    winner= args[2]
    loser= args[3]
    ties_column=args[4]
    session_divider= args[5]
    winner_col_cage_num= args[6]
    loser_col_cage_num=args[7] 
    cols_to_keep= args[8]
    print("header:{}".format(header_row))
    preprocessor = TubeTestDataPreprocessor(raw_data_file_path,cageList,protocol_name, prefix_name)
    preprocessor.main(header_row, winner, loser,ties_column,session_divider,winner_col_cage_num, loser_col_cage_num,cols_to_keep)

    # Use the __all__ attribute to expose the function
    __all__ = ['main']