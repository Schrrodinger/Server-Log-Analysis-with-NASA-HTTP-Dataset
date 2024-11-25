import streamlit as st
from plot import plot_page  # Import the function from plot.py
from tolp import tolp_page  # Import the function from tolp.py

# Define the pages
PAGES = {
    "Page 1": plot_page,
    "Page 2": tolp_page
}

# Define the main function
def main():
    # Set page configuration here
    st.set_page_config(page_title="NASA Logs Analysis", layout="wide")

    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Select a page", list(PAGES.keys()))

    # Call the corresponding function for the selected page
    if page in PAGES:
        PAGES[page]()  # Call the function

# Run the main function
if __name__ == "__main__":
    main()