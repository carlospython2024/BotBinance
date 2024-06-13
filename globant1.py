import ast


def reorder_list_by_hierarchy(lista, n):
    return lista[n:] + lista[:n]


def check_pair_winner(player_one_move, player_two_move):
    hierarchy = ['l', 's', 'r', 'v', 'p']
    position_p_one = hierarchy.index(player_one_move)
    position_p_two = hierarchy.index(player_two_move)
    if player_one_move != player_two_move:
        new_position = position_p_one + 3 if position_p_one < 3 else position_p_one - 2
        new_list = reorder_list_by_hierarchy(hierarchy, new_position)
        player_one_hierarchy = new_list.index(player_one_move)
        player_two_hierarchy = new_list.index(player_two_move)
        if player_one_hierarchy > player_two_hierarchy:
            return "win"
        else:
            return "loose"
    else:
        return "draw"


example_list_one = "[['r', 'v', 's', 's'], ['l', 'p', 'r', 'v']]"
example_list_two = "[['l', 'v', 'r'], ['p', 'r', 's']]"
# You can use one of this list to test or create a new one
lists_to_check = ast.literal_eval(example_list_one)

player_one_wins = 0
player_two_wins = 0
for delta, player_one_element in enumerate(lists_to_check[0]):
    checked_pair = check_pair_winner(player_one_element, lists_to_check[1][delta])
    if checked_pair == "win":
        player_one_wins += 1
    elif checked_pair == "loose":
        player_two_wins += 1

if player_one_wins > player_two_wins:
    print("player 1")
elif player_two_wins > player_one_wins:
    print("player 2")
else:
    print("tie")
