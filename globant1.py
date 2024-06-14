
on = True
miCart = {}

while on:
    product_name = input("Please input the name of the product? ")
    price = float(input(f"Please input the price of {product_name} "))
    quantity = int(input(f"Quantity of {product_name} "))
    discount = float(input(f"Discount to apply to {product_name}? "))
    add_more_products = input(f"Do you like to add another product, press key 'y' for yes, 'n' for no? ")
    miCart[product_name] = {
        'price': price,
        'quantity': quantity,
        'discount': discount
    }

    if add_more_products != 'y':
        print("Thank you!")
        on = False


def check_valid_number(input_number, field):
    while True:
        try:
            valid_number = int(input_number)
            break
        except ValueError:
            print(f"Error, please input a valid number for the {field}")


def calculate_final_price(miCart):
    total_price = 0
    for key, value in miCart.items():
        price_by_quantity = value['price'] * value['quantity']
        price_discount = price_by_quantity * value['discount']
        partial = price_by_quantity - price_discount
        print(f"Partial for {key}: {partial}")
        total_price =+ partial
    print(f"The final price is: {total_price}")


calculate_final_price(miCart)
