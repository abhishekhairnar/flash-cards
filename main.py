from tkinter import *
import pandas
import random

BACKGROUND_COLOR = "#B1DDC6"
try:
    data = pandas.read_csv("words_to_learn.csv")
except FileNotFoundError:

    data = pandas.read_csv("marathi words.csv", encoding="utf-16")
    english = pandas.read_csv("english words.csv")
    data = data.join(english["English"])

to_learn = data.to_dict(orient="records")
current_card = {}


# ____________________________________________CREATING FLASHCARDS___________________________________________
def next_card():
    global current_card, flip_timer
    window.after_cancel(flip_timer)
    current_card = random.choice(to_learn)
    card_canvas.itemconfig(card_title, text="Marathi", fill="black")
    card_canvas.itemconfig(card_word, text=current_card["Marathi"], fill="black")
    card_canvas.itemconfig(card_background, image=card_front_img)
    flip_timer = window.after(3000, func=flip_card)


# ____________________________________________FLIP CARD_____________________________________________________
def flip_card():
    card_canvas.itemconfig(card_title, text="English", fill="white")
    card_canvas.itemconfig(card_word, text=current_card["English"], fill="white")
    card_canvas.itemconfig(card_background, image=card_back_img)


# ____________________________________________RIGHT BUTTON__________________________________________________
def is_known():
    to_learn.remove(current_card)

    data = pandas.DataFrame(to_learn)
    data.to_csv("data/words_to_learn.csv", index=False)

    next_card()


# ____________________________________________UI SETUP______________________________________________________
window = Tk()
window.title("Flashy")
window.config(width=800, height=526, pady=30, padx=50, bg=BACKGROUND_COLOR)

flip_timer = window.after(3000, func=flip_card)

card_back_img = PhotoImage(file="card_back.png")
card_front_img = PhotoImage(file="card_front.png")
wrong_img = PhotoImage(file="wrong.png")
right_img = PhotoImage(file="right.png")

card_canvas = Canvas(width=800, height=526, bg=BACKGROUND_COLOR, highlightthickness=0)

card_background = card_canvas.create_image(400, 263, image=card_front_img)
card_title = card_canvas.create_text(400, 150, text="", font=("Ariel", 40, "italic"))
card_word = card_canvas.create_text(400, 263, text="", font=("Ariel", 60, "bold"))
card_canvas.grid(row=0, column=0, columnspan=2)

wrong_button = Button(image=wrong_img, highlightthickness=0, command=next_card)
wrong_button.grid(row=1, column=0)

right_button = Button(image=right_img, highlightthickness=0, command=is_known)
right_button.grid(row=1, column=1)

next_card()
window.mainloop()
