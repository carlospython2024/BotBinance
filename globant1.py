import ast


def reorder_list(list, n):
    return list[n:] + list[:n]


arr_string = input("Input an array of integers, example [1, 2, 3]: ")
lists_to_check = ast.literal_eval(arr_string)

positions_to_move = int(input("Input the number of positions to rotate the array. "))
direction = input("Press < to move the array to the left or > to move the array to the right. ")

if positions_to_move < len(lists_to_check) and (direction == "<" or direction == ">"):
    positions_to_move = positions_to_move * -1 if direction == "<" else positions_to_move
    new_list = reorder_list(lists_to_check, positions_to_move)
    print(new_list)
else:
    print("Out of the range, you can't move the array, or select a proper direction.")

