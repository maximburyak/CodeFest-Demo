using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Shared;
using System;
using System.Linq;

namespace MapReduceIndexDemo
{
	public class CompanyOrdersTotal
	{
		public string CompanyId { get; set; }
		public double Total { get; set; }
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

	//yields exactly the same results as CompanyOrderTotalIndex with the same performance
	public class CompanyOrderTotalIndex2 : AbstractIndexCreationTask<Order, CompanyOrdersTotal>
	{
		public CompanyOrderTotalIndex2()
		{
			Map = orders => from order in orders
							select new CompanyOrdersTotal
							{
								CompanyId = order.Company,
								Total = order.Lines.Sum(orderLine => orderLine.Quantity * (orderLine.PricePerUnit - orderLine.Discount))
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
		static void Main()
		{
			
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8080",
				DefaultDatabase = "Northwind"
			})
			{			
				store.Initialize();

				new CompanyOrderTotalIndex().Execute(store);
				new CompanyOrderTotalIndex2().Execute(store);

				using (var session = store.OpenSession())
				{
					var query1 = session.Query<CompanyOrdersTotal, CompanyOrderTotalIndex>()
						.Customize(cust => cust.WaitForNonStaleResults())
						.Include(x => x.CompanyId)
						.OrderByDescending(x => x.Total)
						.Take(3)
						.ToList();

					Console.WriteLine("CompanyOrderTotalIndex index - query output");
					foreach (var companyOrdersTotal in query1)
					{
						//since with the query Include() was called - this will be loaded from session cache
						var company = session.Load<dynamic>(companyOrdersTotal.CompanyId);
						Console.WriteLine("CompanyId : {0}, Company Name: {1}, Total: {2}",companyOrdersTotal.CompanyId, company.Name, companyOrdersTotal.Total);
					}

					var query2 = session.Query<CompanyOrdersTotal, CompanyOrderTotalIndex2>()
						.Customize(cust => cust.WaitForNonStaleResults())
						.Include(x => x.CompanyId)
						.OrderByDescending(x => x.Total)
						.Take(3)
						.ToList();

					Console.WriteLine();
					Console.WriteLine("CompanyOrderTotalIndex2 index - query output");
					foreach (var companyOrdersTotal in query2)
					{
						//since with the query Include() was called - this will be loaded from session cache
						var company = session.Load<dynamic>(companyOrdersTotal.CompanyId);
						Console.WriteLine("CompanyId : {0}, Company Name: {1}, Total: {2}", companyOrdersTotal.CompanyId, company.Name, companyOrdersTotal.Total);
					}
	
				}
			}
		}
	}
}
