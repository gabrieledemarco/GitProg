import streamlit as st
from UsersDAO import UsersService
from DbService import DbService
import streamlit_authenticator as stauth


def main():
    Dbs = DbService()
    niknames = Dbs.get_all_value_in_column(name_column='nickname', name_table='users')
    Passwords = Dbs.get_all_value_in_column(name_column='password', name_table='users')

    Sign(passwords=Passwords, names=niknames, usernames=niknames)


def Sign(passwords: list, names: list, usernames: list):
    L = st.sidebar.expander(label="Log In", expanded=False)
    with L:
        st.write("we")
        #Log_in_form(passwords=passwords, names=names, usernames=usernames)
    Sign_page_req = st.sidebar.button("Sign Up")

    if Sign_page_req:
        Sign_up_form()


def Sign_up_form():
    st.title("Welcome dear Binancer")
    st.write("Please insert your Binance Api Key and a valid nickname")

    with st.form("New user Registration", clear_on_submit=False):
        with st.container():
            c1, c2 = st.columns(2)
            with c1:
                nick_name = st.text_input(label="Nickname", max_chars=10, key="name",
                                          help="Insert a valid nickname for your account")
            with c2:
                password = st.text_input(label="Password", max_chars=10, key="pass",
                                         help="Insert a valid password for your account", type="password")
        with st.container():
            c1, c2 = st.columns(2)
            with c1:
                ApiKey = st.text_input(label="Api Key", key="Akey",
                                       help="Please insert your Binance Api Key")
            with c2:
                ApiSec = st.text_input(label="Secret Key", key="Skey",
                                       help="Please insert your Binance Secret Api Key", type="password")

        submitted = st.form_submit_button("Submit")
        if submitted:
            UsersService(api_key=ApiKey, api_secret=ApiSec, nick_name=nick_name, pass_word=password).insert_new_user_and_data()
    return


def Log_in_form(passwords: list, names: list, usernames: list):
        try:
            hashed_passwords = stauth.hasher(passwords).generate()
            authenticator = stauth.authenticate(names, usernames, hashed_passwords,
                                                'some_cookie_name', 'some_signature_key', cookie_expiry_days=30)
            name, authentication_status = authenticator.login('Login', 'main')
        except Exception:
            print("")
        return


main()
