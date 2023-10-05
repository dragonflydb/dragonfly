#include <iostream>
#include <string>
#include <cstdlib>
#include <ctime>

using namespace std;

// Function to generate a random password
string GeneratePassword(int length) {
    const string characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_+";

    srand(static_cast<unsigned int>(time(nullptr)));
    string password;

    for (int i = 0; i < length; ++i) {
        int random_index = rand() % characters.length();
        password += characters[random_index];
    }

    return password;
}

int main() {
    int passwordLength;
    
    cout << "Enter the length of the password: ";
    cin >> passwordLength;

    if (passwordLength <= 0) {
        cout << "Password length should be greater than 0." << endl;
        return 1;
    }

    string password = GeneratePassword(passwordLength);
    
    cout << "Generated Password: " << password << endl;

    return 0;
}
