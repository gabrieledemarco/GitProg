import streamlit as st
import streamlit_authenticator as stauth

from DbService import DbService
from UsersDAO import UsersDAO


def main():
    Dbs = DbService()
    nicknames = Dbs.get_all_value_in_column(name_column='nickname', name_table='users')
    Passwords = Dbs.get_all_value_in_column(name_column='password', name_table='users')

    Sign(nicknames,Passwords)


def Sign(nicknames:list,password:list):
    side = st.sidebar
    with side:
        auth = side.container()
        with auth:
            st.subheader("LogIn/SignUp")
            st.write("Please login in your account \n or register your API and connect to your binance account")
            Log_request = auth.expander(label="Log In", expanded=False)
            with Log_request:
                Log_in_form(nicknames,Passwords)
                
            Sign_request = auth.expander(label="Sign Up", expanded=False)
            with Sign_request:
                Sign_up()
                
           


def Sign_up():
    st.title("Welcome dear Binancer")
    st.write("Please insert your Binance Api Key and a valid nickname")
    New_user_Registration = st.form(key="New_user_Registration", clear_on_submit=True)

    with New_user_Registration:
        with st.container():
            c11, c12 = st.columns(2)
            with c11:
                nick = st.text_input(label="Nickname", max_chars=10)
            with c12:
                password = st.text_input(label="Password", max_chars=10, type="password")

        with st.container():
            c21, c22 = st.columns(2)
            with c21:
                ApiKey = st.text_input(label="Api Key")
            with c22:
                ApiSec = st.text_input(label="Secret Key", type="password")

        submit_button = st.form_submit_button(label='Submit')

        if submit_button:
            Usr = UsersDAO(api_key=ApiKey, api_secret=ApiSec, nick_name=nick, pass_word=password)
            if not Usr.is_user_registered():
                Usr.insert_new_user_and_data()
                st.success(f"Hello dear {nick}, you have successufully registered")

            elif Usr.is_user_registered():
                st.warning(f"Please, choose a different nickname")
            else:
                st.warning("something goes wrong")


def Log_in_form(nicknames:list,password:list):

    try:
        hashed_passwords = stauth.hasher(Passwords).generate()
        authenticator = stauth.authenticate(niknames, niknames, hashed_passwords,
                                            'some_cookie_name', 'some_signature_key', cookie_expiry_days=30)
        name, authentication_status = authenticator.login('Login', 'main')

        if authentication_status is False:
            st.error("Username or Password are incorrect")
        elif authentication_status is True:
            st.success(f"User Authorized, welcome dear {name} ")
        elif authentication_status is None:
            st.info("Please access to your account with a Username and Password")

    except Exception as ex:
        print(ex)


main()
