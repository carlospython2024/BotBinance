# import pandas
#
# nato_csv = pandas.read_csv("nato_phonetic_alphabet.csv")
#
# nato_dict = {row.letter: row.code for (index, row) in nato_csv.iterrows()}
#
# word = input("Enter a word: ")
# nato_result = [nato_dict[letter.upper()] for letter in word]
# print(nato_result)

# String que representa una lista de listas

import ast
import tty
import sys
import termios

# def get_arrow_key():
#     fd = sys.stdin.fileno()
#     old_settings = termios.tcgetattr(fd)
#     try:
#         tty.setraw(sys.stdin.fileno())
#         ch = sys.stdin.read(1)
#     finally:
#         termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
#     return ch
#
# arrow_key = None
# while arrow_key != 'q':
#     arrow_key = get_arrow_key()
#     if arrow_key == '\x1b[D':  # Flecha izquierda
#         print('Izquierda')
#     elif arrow_key == '\x1b[C':  # Flecha derecha
#         print('Derecha')
#     elif arrow_key == 'q':
#         break


def reorder_list(lista, n):
    return list[n:] + list[:n]


arr_string = input("Input an array of integers, example [1, 2, 3]: ")
lists_to_check = ast.literal_eval(arr_string)

positions_to_move = int(input("Input the number of positions to rotate the array."))
if positions_to_move < len(lists_to_check):
    new_list = reorder_list(lists_to_check, positions_to_move)
    print(new_list)
else:
    print("Out of the range, you can't move the array.")

# direction = input("Use the arrow left or right to select direction to rotate the array")
# print(f"La dirección que ingresaste es: {direction}")
