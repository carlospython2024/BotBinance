
string = input("Input a string of dogs with color black or white 1 for black and 0 for \n" 
               " white and I will tell you the number of steps to reorder by color,"
               " example 10010 you'll get 4 steps \n: ")

onehundred_count = string.count('100')
ten_count = string.count('10')

print((onehundred_count * 3) + ((ten_count - onehundred_count) * 2) - 1 if onehundred_count > 0 else ten_count)
