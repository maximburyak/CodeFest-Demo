using FizzWare.NBuilder;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MapReduceIndexDemo
{
	public class CompanyOrdersTotal
	{
		public string CompanyId { get; set; }
		public decimal Total { get; set; }
	}

	public class CompanyOrderTotalIndex : AbstractIndexCreationTask<Order, CompanyOrdersTotal>
	{
		public CompanyOrderTotalIndex()
		{
			Map = orders => from order in orders
							from orderLine in order.Lines
							select new CompanyOrdersTotal
							{
								CompanyId = order.Company,
								Total = orderLine.Quantity * (orderLine.PricePerUnit - orderLine.Discount)
							};

			Reduce = results => from result in results
								group result by result.CompanyId
									into g
								select new CompanyOrdersTotal
								{
									CompanyId = g.Key,
									Total = g.Sum(x => x.Total)
								};

		}
	}

	class Program
	{
		static void Main(string[] args)
		{
			using (var store = new EmbeddableDocumentStore())
			{
				store.Initialize();
				GenerateTestData(store);

				new CompanyOrderTotalIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					var query = session.Query<CompanyOrdersTotal, CompanyOrderTotalIndex>()
						.Customize(cust => cust.WaitForNonStaleResults())
						.Include(x => x.CompanyId)
						.OrderByDescending(x => x.Total)
						.Take(3)
						.ToList();

					foreach (var companyOrdersTotal in query)
					{
						Console.WriteLine("CompanyId : {0}, Total: {1}",companyOrdersTotal.CompanyId, companyOrdersTotal.Total);
					}
				}
			}
		}

		private static void GenerateTestData(IDocumentStore store)
		{
			var companies = GenerateCompanyEntries();
			var orders = GenerateOrderEntries(companies);

			using (var session = store.OpenSession())
			{
				foreach (var company in companies)
					session.Store(company);
				foreach (var order in orders)
					session.Store(order);

				session.SaveChanges();
			}
		}

		private static IList<Company> GenerateCompanyEntries()
		{
			var generator = new SequentialGenerator<int> { Direction = GeneratorDirection.Ascending, Increment = 1 };
			return Builder<Company>.CreateListOfSize(10)
								   .All()
									  .With(obj => obj.Id = "company/" + generator.Generate())
									  .With(obj => obj.Address = Builder<Address>.CreateNew().Build())
								   .Build();
		}

		private static IEnumerable<Order> GenerateOrderEntries(IEnumerable<Company> companies)
		{
			var generator = new SequentialGenerator<int> { Direction = GeneratorDirection.Ascending, Increment = 1 };
			var random = new Random(234);
			var orders = new List<Order>();
			foreach (var company in companies)
			{
				var currentCompany = company;
				var currentOrderLines = Builder<OrderLine>.CreateListOfSize(random.Next(1,15))
														  .All()
															.With(obj => obj.Quantity = random.Next(1,25))
															.With(obj => obj.PricePerUnit = random.Next(10,100))
															.With(obj => obj.Discount = random.Next(5,50))
														  .Build()
														  .ToList();

				var ordersOfCompany = Builder<Order>.CreateListOfSize(random.Next(1,20))
					.All()
						.With(obj => obj.Id = "order/" + generator.Generate())
						.With(obj => obj.Company = currentCompany.Id)
						.With(obj => obj.Lines = currentOrderLines)
					.Build();

				orders.AddRange(ordersOfCompany);
			}

			return orders;
		}
	}
}
