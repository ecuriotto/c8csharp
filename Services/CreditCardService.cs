using System;

using CamundaTraining.Exceptions;
using Microsoft.Extensions.Logging;

namespace CamundaTraining.Services
{
    public class CreditCardService
    {

        public void ChargeAmount(string cardNumber, string cvc, string expiryDate, double amount)
        {
            if (expiryDate.Length == 5)
            {
                Console.WriteLine("Credit card number: " + cardNumber +", CVC: "+cvc+", Expiry date: " + expiryDate + ", Order total: " + amount);
            }
            else
            {
                Console.WriteLine($"The credit card's expiry date is invalid: {expiryDate}");

                throw new InvalidCreditCardException("Invalid credit card expiry date");
            }
        }
    }
}
