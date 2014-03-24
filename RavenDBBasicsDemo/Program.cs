using Raven.Client;
using Raven.Client.Document;
using Shared;
using System;
using System.Linq;

namespace RavenDBBasicsDemo
{
	class Program
	{
		static void Main()
		{
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8080",
				DefaultDatabase = "Northwind"
			})
			{
				store.Initialize();

				StoreNewCompanyRecord(store);

				Console.WriteLine("simple query:");
				SimpleQueryDemo(store);

				Console.WriteLine();
				
				Console.WriteLine("query with include:");
				QueryWithIncludeDemo(store);
			}
		}

		private static void QueryWithIncludeDemo(DocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				var recentOrders = session.Query<Order>()
					.Include(o => o.Company)
					.Where(o => o.OrderedAt.Year >= 1998)
					.Take(5)
					.ToList();

				foreach (var order in recentOrders)
				{
					//in the query there was Include() --> inserts the relevant companies to cache --> Load() loads record from cache
					var company = session.Load<dynamic>(order.Company);
					Console.WriteLine("Order Id: {0}, ordered by company: {1}", order.Id, company.Name);
				}
			}
		}

		private static void SimpleQueryDemo(DocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				var companyNamesQuery = from company in session.Query<Company>()
					where company.Address.Country.Equals("France") ||
					      company.Address.Country.Equals("Israel")
					select company.Name;
			
				foreach (var companyName in companyNamesQuery)
					Console.WriteLine(companyName);
			}
		}

		private static void StoreNewCompanyRecord(DocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				if (!session.Query<Company>().Any(c => c.Name.Equals("Hibernating Rhinos")))
				{
					session.Store(new Company
					{
						Name = "Hibernating Rhinos",
						Contact = new Contact
						{
							Name = "Michael Yarichuk",
							Title = "Software Developer"
						},
						Address = new Address
						{
							City = "Hadera",
							Country = "Israel"
						}
					});
					session.SaveChanges();
				}
			}
		}
	}
}
